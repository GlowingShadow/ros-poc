"""postprocessB: receives frames on /pipeline/frame_to_b, sleeps for an
arbitrary duration (fake processing), stamps 'postprocessB' onto the frame,
and forwards it to /pipeline/frame_to_c. Fully self-contained: no shared code
with manager/postprocess_a/postprocess_c beyond the pipeline_interfaces
message definitions.

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

import cv2
import numpy as np
import rclpy
from pipeline_interfaces.msg import Control, Frame
from rclpy.node import Node
from rclpy.qos import QoSDurabilityPolicy, QoSProfile, QoSReliabilityPolicy
from rclpy.time import Time
from sensor_msgs.msg import Image

ROLE = 'postprocessB'
IN_TOPIC = '/pipeline/frame_to_b'
OUT_TOPIC = '/pipeline/frame_to_c'
CONTROL_TOPIC = '/pipeline/control'
STAMP_SLOT = 1
WORKER_POLL_TIMEOUT_S = 0.5

CONTROL_QOS = QoSProfile(
    depth=1,
    reliability=QoSReliabilityPolicy.RELIABLE,
    durability=QoSDurabilityPolicy.TRANSIENT_LOCAL,
)


def msg_to_image(msg):
    arr = np.frombuffer(bytes(msg.data), dtype=np.uint8)
    return arr.reshape(msg.height, msg.width, 3).copy()


def image_to_msg(img):
    msg = Image()
    msg.height = img.shape[0]
    msg.width = img.shape[1]
    msg.encoding = 'bgr8'
    msg.is_bigendian = 0
    msg.step = msg.width * 3
    msg.data = img.tobytes()
    return msg


def stamp_name_on_frame(img, name, slot_index):
    y = 60 + slot_index * 60
    cv2.putText(img, name, (30, y), cv2.FONT_HERSHEY_SIMPLEX, 1.5, (0, 255, 0), 3, cv2.LINE_AA)
    return img


def random_processing_delay(min_s, max_s):
    return random.uniform(min_s, max_s)


def elapsed_ms(now, stamp_msg):
    return (now - Time.from_msg(stamp_msg)).nanoseconds / 1e6


class PostprocessBNode(Node):

    def __init__(self):
        super().__init__(ROLE)
        self.mode = os.environ.get('MODE', 'copy')
        self.min_delay_s = float(os.environ.get('MIN_PROCESSING_DELAY_S', '0.1'))
        self.max_delay_s = float(os.environ.get('MAX_PROCESSING_DELAY_S', '0.6'))

        self.create_subscription(Control, CONTROL_TOPIC, self.on_control, CONTROL_QOS)
        self.publisher = self.create_publisher(Frame, OUT_TOPIC, 10)
        self.create_subscription(Frame, IN_TOPIC, self.on_frame, 10)

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

        img = stamp_name_on_frame(img, ROLE, STAMP_SLOT)

        out_msg = Frame(
            frame_id=msg.frame_id,
            stamped_by=list(msg.stamped_by) + [ROLE],
            mode=msg.mode,
            origin_stamp=msg.origin_stamp,
            hop_stamp=self.get_clock().now().to_msg(),
        )
        out_msg.image = image_to_msg(img)
        self.publisher.publish(out_msg)
        t_pub = self.get_clock().now()

        transport_ms = elapsed_ms(t_recv, msg.hop_stamp)
        hop_total_ms = elapsed_ms(t_pub, msg.hop_stamp)
        ring_total_ms = elapsed_ms(t_pub, msg.origin_stamp)
        self.get_logger().info(
            f'[{ROLE}] frame_id={msg.frame_id} mode={msg.mode} '
            f'transport_ms={transport_ms:.1f} hop_total_ms={hop_total_ms:.1f} '
            f'ring_total_ms={ring_total_ms:.1f}')

    def stop_worker(self):
        self._stop_event.set()
        self._worker_thread.join()


def main(args=None):
    rclpy.init(args=args)
    node = PostprocessBNode()
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
