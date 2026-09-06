"""postprocessA (metadata branch): subscribes to new_frame, does
NOT modify the image, and instead computes metadata about the frame (size,
mean brightness) which it publishes on metadata_a back to the
manager. Fully self-contained: no shared code with manager/postprocess_b/
postprocess_c beyond the pipeline_interfaces message definitions.

The ROS callback (on_frame) only enqueues work; a background worker thread
does the actual (slow, fake) processing and publishes the result, so the
executor thread stays free to service other callbacks (e.g. on_control)
while a frame is being processed.
"""
import os
import queue
import random
import threading
import time

import numpy as np
import rclpy
from pipeline_interfaces.msg import Control, Frame, Metadata
from rclpy.node import Node
from rclpy.qos import QoSDurabilityPolicy, QoSHistoryPolicy, QoSProfile, QoSReliabilityPolicy
from rclpy.time import Time

ROLE = 'postprocessA'
IN_TOPIC = 'new_frame'
OUT_TOPIC = 'metadata_a'
CONTROL_TOPIC = 'control'
WORKER_POLL_TIMEOUT_S = 0.5

CONTROL_QOS = QoSProfile(
    depth=1,
    reliability=QoSReliabilityPolicy.RELIABLE,
    durability=QoSDurabilityPolicy.TRANSIENT_LOCAL,
)
# Best-effort/volatile for the per-frame data: at real video frame rate, a
# reliable publisher's retransmit/ack bookkeeping on repeated 2.7MB messages
# adds overhead this pipeline doesn't need — dropping an occasional frame is
# preferable to blocking on it. Every node touching new_frame/out_b/out_c/
# metadata_a must use the same QoS, since a reliable subscriber can't match
# a best-effort publisher.
FRAME_QOS = QoSProfile(
    depth=10,
    history=QoSHistoryPolicy.KEEP_LAST,
    reliability=QoSReliabilityPolicy.BEST_EFFORT,
    durability=QoSDurabilityPolicy.VOLATILE,
)


def msg_to_image(msg):
    arr = np.frombuffer(bytes(msg.data), dtype=np.uint8)
    return arr.reshape(msg.height, msg.width, 3).copy()


def random_processing_delay(min_s, max_s):
    return random.uniform(min_s, max_s)


def elapsed_ms(now, stamp_msg):
    return (now - Time.from_msg(stamp_msg)).nanoseconds / 1e6


class PostprocessANode(Node):

    def __init__(self):
        super().__init__(ROLE)
        self.mode = os.environ.get('MODE', 'copy')
        self.min_delay_s = float(os.environ.get('MIN_PROCESSING_DELAY_S', '0.1'))
        self.max_delay_s = float(os.environ.get('MAX_PROCESSING_DELAY_S', '0.6'))

        self.create_subscription(Control, CONTROL_TOPIC, self.on_control, CONTROL_QOS)
        self.publisher = self.create_publisher(Metadata, OUT_TOPIC, FRAME_QOS)
        self.create_subscription(Frame, IN_TOPIC, self.on_frame, FRAME_QOS)

        self._work_queue = queue.Queue()
        self._stop_event = threading.Event()
        self._worker_thread = threading.Thread(target=self._worker_loop, daemon=True)
        self._worker_thread.start()

        self.get_logger().info(f'{ROLE} ready, mode={self.mode}, in={IN_TOPIC}, out={OUT_TOPIC}')

    def on_control(self, msg):
        self.mode = msg.mode
        command = 'RUN' if msg.command == Control.RUN else 'STOP'
        self.get_logger().info(f'control: mode={msg.mode} command={command}')

    def on_frame(self, msg):
        # Runs on the executor thread: capture the receive timestamp (this is
        # what transport_ms is measured from) and hand off immediately so the
        # executor is never blocked by the fake-processing sleep below.
        t_recv = self.get_clock().now()
        self._work_queue.put((msg, t_recv))

    def _worker_loop(self):
        while not self._stop_event.is_set():
            try:
                msg, t_recv = self._work_queue.get(timeout=WORKER_POLL_TIMEOUT_S)
            except queue.Empty:
                continue
            self._process_frame(msg, t_recv)

    def _process_frame(self, msg, t_recv):
        img = msg_to_image(msg.image)

        delay = random_processing_delay(self.min_delay_s, self.max_delay_s)
        time.sleep(delay)

        # Metadata only — the image itself is not modified or re-published.
        mean_brightness = float(img.mean())

        out_msg = Metadata(
            frame_id=msg.frame_id,
            mode=msg.mode,
            width=msg.image.width,
            height=msg.image.height,
            mean_brightness=mean_brightness,
            origin_stamp=msg.origin_stamp,
            hop_stamp=self.get_clock().now().to_msg(),
        )
        self.publisher.publish(out_msg)
        t_pub = self.get_clock().now()

        transport_ms = elapsed_ms(t_recv, msg.hop_stamp)
        hop_total_ms = elapsed_ms(t_pub, msg.hop_stamp)
        self.get_logger().info(
            f'[{ROLE}] frame_id={msg.frame_id} mode={msg.mode} '
            f'mean_brightness={mean_brightness:.1f} '
            f'transport_ms={transport_ms:.1f} hop_total_ms={hop_total_ms:.1f}')

    def stop_worker(self):
        self._stop_event.set()
        self._worker_thread.join()


def main(args=None):
    rclpy.init(args=args)
    node = PostprocessANode()
    try:
        rclpy.spin(node)
    except KeyboardInterrupt:
        pass
    finally:
        node.stop_worker()
        node.destroy_node()
        rclpy.shutdown()


if __name__ == '__main__':
    main()
