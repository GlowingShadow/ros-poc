"""Shared helpers for manager_node and postprocess_node: image generation/
conversion/drawing, topic/role tables, and per-hop latency instrumentation.
"""
import os
import random

import cv2
import numpy as np
from rclpy.time import Time
from sensor_msgs.msg import Image

CONTROL_TOPIC = '/pipeline/control'
MANAGER_OUT_TOPIC = '/pipeline/frame_to_a'
MANAGER_IN_TOPIC = '/pipeline/frame_from_c'

ROLE_TABLE = {
    'postprocessA': {'in': '/pipeline/frame_to_a', 'out': '/pipeline/frame_to_b', 'slot': 0},
    'postprocessB': {'in': '/pipeline/frame_to_b', 'out': '/pipeline/frame_to_c', 'slot': 1},
    'postprocessC': {'in': '/pipeline/frame_to_c', 'out': '/pipeline/frame_from_c', 'slot': 2},
}
EXPECTED_STAMP_ORDER = ['postprocessA', 'postprocessB', 'postprocessC']

_IMAGE_EXTENSIONS = ('.png', '.jpg', '.jpeg')


def gen_sample_images(dir_path, count=5, width=1920, height=1080):
    """Generate `count` synthetic placeholder images in dir_path, unless it
    already contains image files (so real user-supplied images are never
    overwritten and repeated runs don't regenerate/thrash)."""
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
    names = sorted(f for f in os.listdir(dir_path) if f.lower().endswith(_IMAGE_EXTENSIONS))
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


def stamp_name_on_frame(img, name, slot_index):
    y = 60 + slot_index * 60
    cv2.putText(img, name, (30, y), cv2.FONT_HERSHEY_SIMPLEX, 1.5, (0, 255, 0), 3, cv2.LINE_AA)
    return img


def random_processing_delay(min_s=0.1, max_s=0.6):
    return random.uniform(min_s, max_s)


def elapsed_ms(now, stamp_msg):
    """now: rclpy.time.Time: elapsed milliseconds from stamp_msg (builtin_interfaces/Time) to now."""
    return (now - Time.from_msg(stamp_msg)).nanoseconds / 1e6


def log_hop(logger, role, frame_id, mode, hop_stamp, origin_stamp, recv_time, publish_time):
    """Log and return (transport_ms, hop_total_ms, ring_total_ms).

    transport_ms isolates pure messaging/transport cost (recv_time is captured
    before the fake-processing sleep), which is the metric that should differ
    between copy and zero_copy modes. hop_total_ms/ring_total_ms additionally
    include the fake processing delay, for an end-to-end view.
    """
    transport_ms = elapsed_ms(recv_time, hop_stamp)
    hop_total_ms = elapsed_ms(publish_time, hop_stamp)
    ring_total_ms = elapsed_ms(publish_time, origin_stamp)
    logger.info(
        f'[{role}] frame_id={frame_id} mode={mode} '
        f'transport_ms={transport_ms:.1f} hop_total_ms={hop_total_ms:.1f} '
        f'ring_total_ms={ring_total_ms:.1f}')
    return transport_ms, hop_total_ms, ring_total_ms
