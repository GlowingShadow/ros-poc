"""Real-time-paced video decode.

Owns cv2.VideoCapture on a background thread and exposes only the
freshest decoded frame -- cap.read() (H.264 decode) is too heavy to run on
the executor thread, since it would compete with the receive callbacks for
the single spin() thread. Decoding on a real-time schedule into a size-1
"latest frame" slot (overwrite = drop stale) means content stays locked to
the wall clock because *decode* advances on schedule; consumers just take
whatever's freshest, and the executor never touches `cap`.
"""
import threading
import time

import cv2

from .config import DEFAULT_FPS


class PacedVideoSource:

    def __init__(self, video_path):
        self.cap = cv2.VideoCapture(video_path)
        if not self.cap.isOpened():
            raise RuntimeError(f'failed to open video: {video_path}')
        self.width = int(self.cap.get(cv2.CAP_PROP_FRAME_WIDTH))
        self.height = int(self.cap.get(cv2.CAP_PROP_FRAME_HEIGHT))
        fps = self.cap.get(cv2.CAP_PROP_FPS)
        self.fps = fps if fps and fps > 1e-3 else DEFAULT_FPS
        self.send_interval_s = 1.0 / self.fps
        self.estimated_frame_count = max(
            int(self.cap.get(cv2.CAP_PROP_FRAME_COUNT)), 0)

        self._lock = threading.Lock()
        self._latest_frame = None   # (frame_id, img): freshest undelivered decode
        self._eof = False
        self._stop = threading.Event()
        self._thread = None
        self._index = 0

    def start(self):
        self._thread = threading.Thread(target=self._decode_loop, daemon=True)
        self._thread.start()

    def take_latest(self):
        """Return (frame_id, img) for the freshest undelivered decode, or
        None if nothing new has landed since the last call. Consuming
        clears the slot so a frame is never delivered twice."""
        with self._lock:
            item = self._latest_frame
            self._latest_frame = None
            return item

    @property
    def eof(self):
        with self._lock:
            return self._eof

    def _decode_loop(self):
        # Decodes on a fixed real-time schedule into the size-1 slot;
        # overwriting drops frames the consumer didn't grab (live feed shows
        # the newest, not a backlog). Clamps the deadline when behind so a
        # slow decode can't spiral into an ever-growing deficit.
        next_deadline = time.monotonic()
        while not self._stop.is_set():
            ret, img = self.cap.read()
            if not ret:
                with self._lock:
                    self._eof = True
                return
            frame_id = self._index
            self._index += 1
            with self._lock:
                self._latest_frame = (frame_id, img)
            next_deadline += self.send_interval_s
            delay = next_deadline - time.monotonic()
            if delay > 0:
                self._stop.wait(delay)   # interruptible sleep
            else:
                next_deadline = time.monotonic()  # behind: drop the deficit

    def stop(self):
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=2.0)
            self._thread = None
        self.cap.release()
