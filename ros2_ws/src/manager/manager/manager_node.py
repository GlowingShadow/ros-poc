"""manager: reads a video file and publishes it frame-by-frame in real time
on new_frame, which fans out to two branches:

    manager --new_frame--> postprocessA --metadata_a--> manager   (metadata branch)
    manager --new_frame--> postprocessB --out_b--> postprocessC --out_c--> manager  (image chain)

For each frame_id, once BOTH the final stamped image (out_c) and the
metadata (metadata_a) have arrived, the manager burns frame_id and
mean_brightness onto the frame and streams it live over UDP via a
gst-launch-1.0 subprocess (MJPEG-over-RTP, since a raw JPEG frame can
exceed a single UDP datagram). Fully self-contained: no shared code with
the postprocess packages beyond the pipeline_interfaces message
definitions.
"""
import os
import subprocess

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
DEFAULT_FPS = 30.0

CONTROL_QOS = QoSProfile(
    depth=1,
    reliability=QoSReliabilityPolicy.RELIABLE,
    durability=QoSDurabilityPolicy.TRANSIENT_LOCAL,
)


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


def burn_metadata_on_frame(img, frame_id, mean_brightness):
    text = f'frame={frame_id} brightness={mean_brightness:.1f}'
    cv2.putText(img, text, (30, 180), cv2.FONT_HERSHEY_SIMPLEX, 1.5, (0, 255, 255), 3, cv2.LINE_AA)
    return img


def elapsed_ms(now, stamp_msg):
    return (now - Time.from_msg(stamp_msg)).nanoseconds / 1e6


class ManagerNode(Node):

    def __init__(self):
        super().__init__('manager')

        self.mode = os.environ.get('MODE', 'copy')
        self.video_path = os.environ.get('VIDEO_PATH', '/workspace/input/video_720.mp4')
        self.gst_udp_host = os.environ.get('GST_UDP_HOST', 'host.docker.internal')
        self.gst_udp_port = int(os.environ.get('GST_UDP_PORT', '5000'))
        self.timeout_s = float(os.environ.get('COLLECT_TIMEOUT_S', '30.0'))

        self.cap = cv2.VideoCapture(self.video_path)
        if not self.cap.isOpened():
            raise RuntimeError(f'failed to open video: {self.video_path}')
        self.width = int(self.cap.get(cv2.CAP_PROP_FRAME_WIDTH))
        self.height = int(self.cap.get(cv2.CAP_PROP_FRAME_HEIGHT))
        fps = self.cap.get(cv2.CAP_PROP_FPS)
        self.fps = fps if fps and fps > 1e-3 else DEFAULT_FPS
        self.send_interval_s = 1.0 / self.fps
        estimated_frame_count = max(int(self.cap.get(cv2.CAP_PROP_FRAME_COUNT)), 0)

        self.gst_proc = self._start_gst_pipeline()

        self.control_pub = self.create_publisher(Control, CONTROL_TOPIC, CONTROL_QOS)
        self.frame_pub = self.create_publisher(Frame, NEW_FRAME_TOPIC, 10)
        self.create_subscription(Frame, OUT_C_TOPIC, self.on_final_frame, 10)
        self.create_subscription(Metadata, METADATA_A_TOPIC, self.on_metadata, 10)

        # Per frame_id: holds whichever of {'image', 'meta'} has arrived so
        # far. A frame is only burned-in and streamed once both are present,
        # since arrival order between the metadata branch (A) and the image
        # chain (B->C) isn't guaranteed.
        self.pending = {}
        self.image_results = {}
        self.metadata_results = {}
        self.emitted_count = 0
        self.total_frames = None  # known only once EOF is reached
        self.timeout_timer = None
        self.send_timer = None
        self._next_frame_index = 0

        self.get_logger().info(
            f'manager starting, mode={self.mode}, video={self.video_path} '
            f'{self.width}x{self.height}@{self.fps:.2f}fps '
            f'(~{estimated_frame_count} frames estimated)')
        # Wait a beat before the first send so DDS discovery across the
        # containers has time to settle; use a one-shot timer (not
        # time.sleep) so the executor stays responsive the whole time.
        self.startup_timer = self.create_timer(2.0, self._on_startup_timer)
        self._estimated_frame_count = estimated_frame_count

    def _start_gst_pipeline(self):
        cmd = [
            'gst-launch-1.0', '-q',
            'fdsrc', 'fd=0', '!',
            'rawvideoparse',
            f'width={self.width}', f'height={self.height}', 'format=bgr',
            f'framerate={round(self.fps)}/1', '!',
            # rtpjpegpay (RFC 2435) only accepts 4:2:0/4:2:2 JPEGs; jpegenc
            # fed BGR/RGB directly encodes 4:4:4 (no subsampling), which
            # rtpjpegpay rejects with "Invalid component" and silently
            # drops every frame. Forcing I420 first makes jpegenc subsample
            # like a normal encoder, which rtpjpegpay accepts.
            'videoconvert', '!', 'video/x-raw,format=I420', '!',
            'jpegenc', '!',
            'rtpjpegpay', '!',
            'udpsink', f'host={self.gst_udp_host}', f'port={self.gst_udp_port}',
        ]
        self.get_logger().info(f"starting gst pipeline: {' '.join(cmd)}")
        return subprocess.Popen(cmd, stdin=subprocess.PIPE)

    def _on_startup_timer(self):
        self.startup_timer.cancel()
        control_msg = Control(
            mode=self.mode, command=Control.RUN, total_frames=self._estimated_frame_count)
        self.control_pub.publish(control_msg)
        # One frame per timer tick (not a sleep loop) so return callbacks for
        # early frames are never stuck behind later sends.
        self.send_timer = self.create_timer(self.send_interval_s, self._on_send_timer)

    def _on_send_timer(self):
        ret, img = self.cap.read()
        if not ret:
            self.send_timer.cancel()
            self.cap.release()
            self.total_frames = self._next_frame_index
            self.get_logger().info(f'end of video reached, sent {self.total_frames} frame(s)')
            self.timeout_timer = self.create_timer(self.timeout_s, self._on_timeout)
            self._maybe_finish()
            return

        frame_id = self._next_frame_index
        stamp = self.get_clock().now().to_msg()
        frame_msg = Frame(
            frame_id=frame_id, stamped_by=[], mode=self.mode,
            origin_stamp=stamp, hop_stamp=stamp)
        frame_msg.image = image_to_msg(img)
        self.frame_pub.publish(frame_msg)
        self.get_logger().info(f'sent frame_id={frame_id} mode={self.mode}')

        self._next_frame_index += 1

    def on_final_frame(self, msg):
        entry = self.pending.setdefault(msg.frame_id, {})
        if 'image' in entry:
            return  # already handled (or unexpected re-delivery); ignore

        t_recv = self.get_clock().now()
        transport_ms = elapsed_ms(t_recv, msg.hop_stamp)
        ring_total_ms = elapsed_ms(t_recv, msg.origin_stamp)
        ok = list(msg.stamped_by) == EXPECTED_STAMP_ORDER
        self.get_logger().info(
            f'[manager] image frame_id={msg.frame_id} mode={msg.mode} '
            f'transport_ms={transport_ms:.1f} ring_total_ms={ring_total_ms:.1f} '
            f"{'PASS' if ok else 'FAIL'} stamped_by={list(msg.stamped_by)}")

        entry['image'] = msg_to_image(msg.image)
        entry['image_stats'] = {
            'transport_ms': transport_ms, 'ring_total_ms': ring_total_ms, 'ok': ok,
        }
        self._maybe_emit(msg.frame_id)

    def on_metadata(self, msg):
        entry = self.pending.setdefault(msg.frame_id, {})
        if 'meta' in entry:
            return  # already handled (or unexpected re-delivery); ignore

        t_recv = self.get_clock().now()
        transport_ms = elapsed_ms(t_recv, msg.hop_stamp)
        self.get_logger().info(
            f'[manager] metadata frame_id={msg.frame_id} mode={msg.mode} '
            f'transport_ms={transport_ms:.1f} {msg.width}x{msg.height} '
            f'mean_brightness={msg.mean_brightness:.1f}')

        entry['meta'] = {
            'transport_ms': transport_ms,
            'width': msg.width, 'height': msg.height,
            'mean_brightness': msg.mean_brightness,
        }
        self._maybe_emit(msg.frame_id)

    def _maybe_emit(self, frame_id):
        entry = self.pending.get(frame_id)
        if entry is None or 'image' not in entry or 'meta' not in entry:
            return

        img = burn_metadata_on_frame(entry['image'], frame_id, entry['meta']['mean_brightness'])
        try:
            self.gst_proc.stdin.write(img.tobytes())
        except (BrokenPipeError, ValueError):
            self.get_logger().warning(f'gst pipeline stdin closed; frame_id={frame_id} dropped')

        self.image_results[frame_id] = entry['image_stats']
        self.metadata_results[frame_id] = entry['meta']
        del self.pending[frame_id]
        self.emitted_count += 1
        self._maybe_finish()

    def _maybe_finish(self):
        if self.total_frames is not None and self.emitted_count >= self.total_frames:
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

        stop_msg = Control(mode=self.mode, command=Control.STOP, total_frames=self.total_frames)
        self.control_pub.publish(stop_msg)

        self.stop_gst()
        self.get_logger().info('manager finished all frames, shutting down')
        if rclpy.ok():
            rclpy.shutdown()

    def stop_gst(self):
        if self.gst_proc is None:
            return
        try:
            if self.gst_proc.stdin:
                self.gst_proc.stdin.close()
            self.gst_proc.wait(timeout=5.0)
        except Exception:
            self.gst_proc.kill()
        finally:
            self.gst_proc = None

    def _on_timeout(self):
        self.timeout_timer.cancel()
        missing = sorted(set(range(self.total_frames)) - set(self.image_results))
        if missing:
            self.get_logger().warning(f'timed out; missing frame_id(s)={missing}')


def main(args=None):
    rclpy.init(args=args)
    node = ManagerNode()
    try:
        rclpy.spin(node)
    except KeyboardInterrupt:
        pass
    finally:
        try:
            node.stop_gst()
            node.cap.release()
            node.destroy_node()
        except Exception:
            pass
        if rclpy.ok():
            rclpy.shutdown()


if __name__ == '__main__':
    main()
