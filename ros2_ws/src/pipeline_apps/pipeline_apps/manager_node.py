"""Generator + collector role: sends the sample image set once through the
ring (manager -> A -> B -> C -> manager) and verifies/logs the result."""
import os

import rclpy
from pipeline_interfaces.msg import Control, Frame
from rclpy.node import Node
from rclpy.qos import QoSDurabilityPolicy, QoSProfile, QoSReliabilityPolicy

from pipeline_apps import common

CONTROL_QOS = QoSProfile(
    depth=1,
    reliability=QoSReliabilityPolicy.RELIABLE,
    durability=QoSDurabilityPolicy.TRANSIENT_LOCAL,
)


class ManagerNode(Node):

    def __init__(self):
        super().__init__('manager')

        self.mode = os.environ.get('MODE', 'copy')
        self.sample_dir = os.environ.get('SAMPLE_IMAGES_DIR', '/workspace/sample_images')
        self.output_dir = os.environ.get('OUTPUT_FRAMES_DIR', '/workspace/output_frames')
        num_sample_images = int(os.environ.get('NUM_SAMPLE_IMAGES', '5'))
        self.send_interval_s = float(os.environ.get('SEND_INTERVAL_S', '0.5'))
        self.timeout_s = float(os.environ.get('COLLECT_TIMEOUT_S', '30.0'))

        common.gen_sample_images(self.sample_dir, count=num_sample_images)
        image_paths = common.list_sample_images(self.sample_dir)
        if not image_paths:
            raise RuntimeError(f'no sample images found/generated in {self.sample_dir}')
        self.images = [common.load_image(p) for p in image_paths]
        os.makedirs(self.output_dir, exist_ok=True)

        self.control_pub = self.create_publisher(Control, common.CONTROL_TOPIC, CONTROL_QOS)
        self.frame_pub = self.create_publisher(Frame, common.MANAGER_OUT_TOPIC, 10)
        self.create_subscription(Frame, common.MANAGER_IN_TOPIC, self.on_final_frame, 10)

        self.pending = set(range(len(self.images)))
        self.results = {}
        self.timeout_timer = None
        self.send_timer = None
        self._next_frame_index = 0

        self.get_logger().info(
            f'manager starting, mode={self.mode}, {len(self.images)} sample image(s)')
        # Wait a beat before the first send so DDS discovery across the 4
        # containers has time to settle; use a one-shot timer (not
        # time.sleep) so the executor stays responsive the whole time.
        self.startup_timer = self.create_timer(2.0, self._on_startup_timer)

    def _on_startup_timer(self):
        self.startup_timer.cancel()
        control_msg = Control(mode=self.mode, command=Control.RUN, total_frames=len(self.images))
        self.control_pub.publish(control_msg)
        # One frame per timer tick (not a sleep loop) so on_final_frame
        # callbacks for early frames are never stuck behind later sends.
        self.send_timer = self.create_timer(self.send_interval_s, self._on_send_timer)

    def _on_send_timer(self):
        frame_id = self._next_frame_index
        stamp = self.get_clock().now().to_msg()
        frame_msg = Frame(
            frame_id=frame_id, stamped_by=[], mode=self.mode,
            origin_stamp=stamp, hop_stamp=stamp)
        frame_msg.image = common.image_to_msg(self.images[frame_id])
        self.frame_pub.publish(frame_msg)
        self.get_logger().info(f'sent frame_id={frame_id} mode={self.mode}')

        self._next_frame_index += 1
        if self._next_frame_index >= len(self.images):
            self.send_timer.cancel()
            self.timeout_timer = self.create_timer(self.timeout_s, self._on_timeout)

    def on_final_frame(self, msg):
        if msg.frame_id not in self.pending:
            return  # already handled (or unexpected re-delivery); ignore

        t_recv = self.get_clock().now()
        transport_ms, _, ring_total_ms = common.log_hop(
            self.get_logger(), 'manager', msg.frame_id, msg.mode,
            msg.hop_stamp, msg.origin_stamp, t_recv, t_recv)

        ok = list(msg.stamped_by) == common.EXPECTED_STAMP_ORDER
        self.get_logger().info(
            f"frame_id={msg.frame_id} {'PASS' if ok else 'FAIL'} stamped_by={list(msg.stamped_by)}")

        img = common.msg_to_image(msg.image)
        out_path = os.path.join(self.output_dir, f'frame_{msg.frame_id}_{msg.mode}.png')
        common.save_image(out_path, img)

        self.results[msg.frame_id] = {
            'transport_ms': transport_ms, 'ring_total_ms': ring_total_ms, 'ok': ok,
        }
        self.pending.discard(msg.frame_id)

        if not self.pending:
            if self.timeout_timer is not None:
                self.timeout_timer.cancel()
            self.print_summary()

    def print_summary(self):
        self.get_logger().info('=== summary ===')
        self.get_logger().info('frame_id | transport_ms | ring_total_ms | status')
        for frame_id in sorted(self.results):
            r = self.results[frame_id]
            self.get_logger().info(
                f"{frame_id:>8} | {r['transport_ms']:>12.1f} | "
                f"{r['ring_total_ms']:>13.1f} | {'PASS' if r['ok'] else 'FAIL'}")
        avg_transport = sum(r['transport_ms'] for r in self.results.values()) / len(self.results)
        avg_total = sum(r['ring_total_ms'] for r in self.results.values()) / len(self.results)
        self.get_logger().info(
            f'average transport_ms={avg_transport:.1f} average ring_total_ms={avg_total:.1f}')

        stop_msg = Control(mode=self.mode, command=Control.STOP, total_frames=len(self.images))
        self.control_pub.publish(stop_msg)

        self.get_logger().info('manager finished all frames, shutting down')
        if rclpy.ok():
            rclpy.shutdown()

    def _on_timeout(self):
        self.timeout_timer.cancel()
        if self.pending:
            self.get_logger().warning(f'timed out waiting for frame_id(s): {sorted(self.pending)}')


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
