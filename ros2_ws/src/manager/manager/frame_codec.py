"""Frame message <-> numpy-image conversions, and the on-frame overlay."""
import cv2
import numpy as np
from rclpy.time import Time

from .config import FRAME_DATA_LEN, FRAME_HEIGHT, FRAME_WIDTH


def image_to_msg(img):
    # Frame.image_data is a fixed-size array (uint8[2764800]) -- unlike the
    # old dynamic uint8[] field, its generated setter does
    # numpy.array(value, dtype=uint8), which does NOT treat `bytes` as a
    # buffer of ints (raises ValueError). np.frombuffer is required here.
    assert img.nbytes == FRAME_DATA_LEN, f'expected {FRAME_DATA_LEN} bytes, got {img.nbytes}'
    return np.frombuffer(img.tobytes(), dtype=np.uint8)


def msg_to_image(msg):
    # msg.image_data is already a numpy uint8 array (rosidl generates numpy
    # storage for fixed-size numeric arrays -- see image_to_msg). reshape is a
    # view; the single .copy() gives cv2.putText a writable buffer that stays
    # valid after `msg` is GC'd. The old bytes(msg.image_data) was a second
    # full 2.7MB copy for nothing; np.asarray keeps this a no-op for numpy
    # input while still handling array.array from other rmw impls.
    return np.asarray(msg.image_data, dtype=np.uint8).reshape(
        FRAME_HEIGHT, FRAME_WIDTH, 3).copy()


def burn_metadata_on_frame(img, frame_id, mean_brightness):
    # Split across two lines rather than one "label: text" line -- at this
    # font scale the combined string renders ~1494px wide, wider than the
    # 1280px frame, and would run off the right edge.
    label = 'manager (from postprocessA)'
    text = f'frame={frame_id} brightness={mean_brightness:.1f}'
    cv2.putText(img, label, (30, 180), cv2.FONT_HERSHEY_SIMPLEX,
                1.5, (0, 255, 255), 3, cv2.LINE_AA)
    cv2.putText(img, text, (30, 225), cv2.FONT_HERSHEY_SIMPLEX,
                1.5, (0, 255, 255), 3, cv2.LINE_AA)
    return img


def elapsed_ms(now, stamp_msg):
    return (now - Time.from_msg(stamp_msg)).nanoseconds / 1e6
