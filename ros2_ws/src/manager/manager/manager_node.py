"""manager: generates the sample frame set and publishes it once on
new_frame, which fans out to two branches:

    manager --new_frame--> postprocessA --metadata_a--> manager   (metadata branch)
    manager --new_frame--> postprocessB --out_b--> postprocessC --out_c--> manager  (image chain)

The manager collects BOTH the final stamped image (out_c) and the metadata
(metadata_a) per frame, and considers a frame done only when both have
arrived. Fully self-contained: no shared code with the postprocess packages
beyond the pipeline_interfaces message definitions.
"""
import os

import cv2
import numpy as np
import rclpy
from pipeline_interfaces.msg import Control, Frame, Metadata
from rclpy.node import Node
from rclpy.qos import QoSDurabilityPolicy, QoSProfile, QoSReliabilityPolicy
from rclpy.time import Time
from sensor_msgs.msg import Image

CONTROL_TOPIC = 'control'
NEW_FRAME_TOPIC = 'new_frame'
OUT_C_TOPIC = 'out_c'
METADATA_A_TOPIC = 'metadata_a'
# The image now only passes through B and C (A produces metadata, not a stamp).
EXPECTED_STAMP_ORDER = ['postprocessB', 'postprocessC']
IMAGE_EXTENSIONS = ('.png', '.jpg', '.jpeg')

CONTROL_QOS = QoSProfile(
    depth=1,
    reliability=QoSReliabilityPolicy.RELIABLE,
    durability=QoSDurabilityPolicy.TRANSIENT_LOCAL,
)


def gen_sample_images(dir_path, count=5, width=1920, height=1080):
    """Generate `count` synthetic placeholder images in dir_path, unless it
    already contains image files (real user-supplied images are never
    overwritten; repeated runs don't regenerate/thrash)."""
    os.makedirs(dir_path, exist_ok=True)
    if list_sample_images(dir_path):
        return

    gradient_row = np.linspace(0, 255, width, dtype=np.uint8)
    for i in range(count):
        img = np.zeros((height, width, 3), dtype=np.uint8)
        img[:, :, 0] = gradient_row
        img[:, :, 1] = (i * 40) % 256
        img[:, :, 2] = 255 - gradient_row
        cv2.putText(
            img, f'SAMPLE {i}', (width // 2 - 320, height // 2),
            cv2.FONT_HERSHEY_SIMPLEX, 4.0, (255, 255, 255), 8, cv2.LINE_AA)
        cv2.imwrite(os.path.join(dir_path, f'sample_{i}.png'), img)


def list_sample_images(dir_path):
    if not os.path.isdir(dir_path):
        return []
    names = sorted(f for f in os.listdir(dir_path) if f.lower().endswith(IMAGE_EXTENSIONS))
    return [os.path.join(dir_path, name) for name in names]


def load_image(path):
    img = cv2.imread(path, cv2.IMREAD_COLOR)
    if img is None:
        raise RuntimeError(f'failed to load image: {path}')
    return img


def save_image(path, img):
    cv2.imwrite(path, img)


def image_to_msg(img):
    msg = Image()
    msg.height = img.shape[0]
    msg.width = img.shape[1]
    msg.encoding = 'bgr8'
    msg.is_bigendian = 0
    msg.step = msg.width * 3
    msg.data = img.tobytes()
    return msg


def msg_to_image(msg):
    arr = np.frombuffer(bytes(msg.data), dtype=np.uint8)
    return arr.reshape(msg.height, msg.width, 3).copy()


def elapsed_ms(now, stamp_msg):
    return (now - Time.from_msg(stamp_msg)).nanoseconds / 1e6


class ManagerNode(Node):

    def __init__(self):
        super().__init__('manager')

        self.mode = os.environ.get('MODE', 'copy')
        self.sample_dir = os.environ.get('SAMPLE_IMAGES_DIR', '/workspace/sample_images')
        self.output_dir = os.environ.get('OUTPUT_FRAMES_DIR', '/workspace/output_frames')
        num_sample_images = int(os.environ.get('NUM_SAMPLE_IMAGES', '5'))
        self.send_interval_s = float(os.environ.get('SEND_INTERVAL_S', '0.5'))
        self.timeout_s = float(os.environ.get('COLLECT_TIMEOUT_S', '30.0'))

        gen_sample_images(self.sample_dir, count=num_sample_images)
        image_paths = list_sample_images(self.sample_dir)
        if not image_paths:
            raise RuntimeError(f'no sample images found/generated in {self.sample_dir}')
        self.images = [load_image(p) for p in image_paths]
        os.makedirs(self.output_dir, exist_ok=True)

        self.control_pub = self.create_publisher(Control, CONTROL_TOPIC, CONTROL_QOS)
        self.frame_pub = self.create_publisher(Frame, NEW_FRAME_TOPIC, 10)
        self.create_subscription(Frame, OUT_C_TOPIC, self.on_final_frame, 10)
        self.create_subscription(Metadata, METADATA_A_TOPIC, self.on_metadata, 10)

        # A frame is "done" only when BOTH the image (out_c) and the metadata
        # (metadata_a) have come back for it.
        self.pending_image = set(range(len(self.images)))
        self.pending_metadata = set(range(len(self.images)))
        self.image_results = {}
        self.metadata_results = {}
        self.timeout_timer = None
        self.send_timer = None
        self._next_frame_index = 0

        self.get_logger().info(
            f'manager starting, mode={self.mode}, {len(self.images)} sample image(s)')
        # Wait a beat before the first send so DDS discovery across the
        # containers has time to settle; use a one-shot timer (not
        # time.sleep) so the executor stays responsive the whole time.
        self.startup_timer = self.create_timer(2.0, self._on_startup_timer)

    def _on_startup_timer(self):
        self.startup_timer.cancel()
        control_msg = Control(mode=self.mode, command=Control.RUN, total_frames=len(self.images))
        self.control_pub.publish(control_msg)
        # One frame per timer tick (not a sleep loop) so return callbacks for
        # early frames are never stuck behind later sends.
        self.send_timer = self.create_timer(self.send_interval_s, self._on_send_timer)

    def _on_send_timer(self):
        frame_id = self._next_frame_index
        stamp = self.get_clock().now().to_msg()
        frame_msg = Frame(
            frame_id=frame_id, stamped_by=[], mode=self.mode,
            origin_stamp=stamp, hop_stamp=stamp)
        frame_msg.image = image_to_msg(self.images[frame_id])
        self.frame_pub.publish(frame_msg)
        self.get_logger().info(f'sent frame_id={frame_id} mode={self.mode}')

        self._next_frame_index += 1
        if self._next_frame_index >= len(self.images):
            self.send_timer.cancel()
            self.timeout_timer = self.create_timer(self.timeout_s, self._on_timeout)

    def on_final_frame(self, msg):
        if msg.frame_id not in self.pending_image:
            return  # already handled (or unexpected re-delivery); ignore

        t_recv = self.get_clock().now()
        transport_ms = elapsed_ms(t_recv, msg.hop_stamp)
        ring_total_ms = elapsed_ms(t_recv, msg.origin_stamp)
        ok = list(msg.stamped_by) == EXPECTED_STAMP_ORDER
        self.get_logger().info(
            f'[manager] image frame_id={msg.frame_id} mode={msg.mode} '
            f'transport_ms={transport_ms:.1f} ring_total_ms={ring_total_ms:.1f} '
            f"{'PASS' if ok else 'FAIL'} stamped_by={list(msg.stamped_by)}")

        img = msg_to_image(msg.image)
        out_path = os.path.join(self.output_dir, f'frame_{msg.frame_id}_{msg.mode}.png')
        save_image(out_path, img)

        self.image_results[msg.frame_id] = {
            'transport_ms': transport_ms, 'ring_total_ms': ring_total_ms, 'ok': ok,
        }
        self.pending_image.discard(msg.frame_id)
        self._maybe_finish()

    def on_metadata(self, msg):
        if msg.frame_id not in self.pending_metadata:
            return  # already handled (or unexpected re-delivery); ignore

        t_recv = self.get_clock().now()
        transport_ms = elapsed_ms(t_recv, msg.hop_stamp)
        self.get_logger().info(
            f'[manager] metadata frame_id={msg.frame_id} mode={msg.mode} '
            f'transport_ms={transport_ms:.1f} {msg.width}x{msg.height} '
            f'mean_brightness={msg.mean_brightness:.1f}')

        self.metadata_results[msg.frame_id] = {
            'transport_ms': transport_ms,
            'width': msg.width, 'height': msg.height,
            'mean_brightness': msg.mean_brightness,
        }
        self.pending_metadata.discard(msg.frame_id)
        self._maybe_finish()

    def _maybe_finish(self):
        if not self.pending_image and not self.pending_metadata:
            if self.timeout_timer is not None:
                self.timeout_timer.cancel()
            self.print_summary()

    def print_summary(self):
        self.get_logger().info('=== summary ===')
        self.get_logger().info(
            'frame_id | img_transport_ms | ring_total_ms | status | '
            'meta_transport_ms | mean_brightness')
        for frame_id in sorted(self.image_results):
            r = self.image_results[frame_id]
            m = self.metadata_results.get(frame_id, {})
            self.get_logger().info(
                f"{frame_id:>8} | {r['transport_ms']:>16.1f} | "
                f"{r['ring_total_ms']:>13.1f} | {'PASS' if r['ok'] else 'FAIL':>6} | "
                f"{m.get('transport_ms', float('nan')):>17.1f} | "
                f"{m.get('mean_brightness', float('nan')):>15.1f}")
        avg_transport = sum(r['transport_ms'] for r in self.image_results.values()) / len(self.image_results)
        avg_total = sum(r['ring_total_ms'] for r in self.image_results.values()) / len(self.image_results)
        self.get_logger().info(
            f'average image transport_ms={avg_transport:.1f} '
            f'average ring_total_ms={avg_total:.1f}')

        stop_msg = Control(mode=self.mode, command=Control.STOP, total_frames=len(self.images))
        self.control_pub.publish(stop_msg)

        self.get_logger().info('manager finished all frames, shutting down')
        if rclpy.ok():
            rclpy.shutdown()

    def _on_timeout(self):
        self.timeout_timer.cancel()
        if self.pending_image or self.pending_metadata:
            self.get_logger().warning(
                f'timed out; missing image frame_id(s)={sorted(self.pending_image)} '
                f'metadata frame_id(s)={sorted(self.pending_metadata)}')


def main(args=None):
    rclpy.init(args=args)
    node = ManagerNode()
    try:
        rclpy.spin(node)
    except KeyboardInterrupt:
        pass
    finally:
        try:
            node.destroy_node()
        except Exception:
            pass
        if rclpy.ok():
            rclpy.shutdown()


if __name__ == '__main__':
    main()
