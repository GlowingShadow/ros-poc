"""Background GStreamer sink: encodes raw BGR frames to MJPEG-over-RTP and
streams them out over UDP via a gst-launch-1.0 subprocess.
"""
import queue
import subprocess
import threading

from .config import WORKER_POLL_TIMEOUT_S


class GstSink:

    def __init__(self, width, height, fps, host, port, logger):
        self._logger = logger
        self._queue = queue.Queue()
        self._stop_event = threading.Event()
        self._proc = self._start_pipeline(width, height, fps, host, port)
        # gst-launch's stdin pipe is only ~64KB; writing a full 2.7MB raw
        # frame blocks until gst-launch drains enough of it. Doing that
        # write directly on a ROS callback would stall the whole executor
        # (and therefore every incoming topic) for as long as encoding one
        # frame takes, so a background worker thread owns the actual write
        # -- same pattern postprocessB/C use for their fake-processing sleep.
        self._thread = threading.Thread(target=self._worker_loop, daemon=True)
        self._thread.start()

    def _start_pipeline(self, width, height, fps, host, port):
        cmd = [
            'gst-launch-1.0', '-q',
            'fdsrc', 'fd=0', '!',
            'rawvideoparse',
            f'width={width}', f'height={height}', 'format=bgr',
            f'framerate={round(fps)}/1', '!',
            # rtpjpegpay (RFC 2435) only accepts 4:2:0/4:2:2 JPEGs; jpegenc
            # fed BGR/RGB directly encodes 4:4:4 (no subsampling), which
            # rtpjpegpay rejects with "Invalid component" and silently
            # drops every frame. Forcing I420 first makes jpegenc subsample
            # like a normal encoder, which rtpjpegpay accepts.
            'videoconvert', '!', 'video/x-raw,format=I420', '!',
            'jpegenc', '!',
            'rtpjpegpay', '!',
            'udpsink', f'host={host}', f'port={port}',
        ]
        self._logger.info(f"starting gst pipeline: {' '.join(cmd)}")
        return subprocess.Popen(cmd, stdin=subprocess.PIPE)

    def send(self, img):
        self._queue.put(img)

    def _worker_loop(self):
        # Keep draining even after stop is requested, so frames already
        # queued by the time shutdown starts still reach gst-launch.
        while not self._stop_event.is_set() or not self._queue.empty():
            try:
                img = self._queue.get(timeout=WORKER_POLL_TIMEOUT_S)
            except queue.Empty:
                continue
            try:
                self._proc.stdin.write(img.tobytes())
            except (BrokenPipeError, ValueError):
                self._logger.warning(
                    'gst pipeline stdin closed; frame dropped')

    def stop(self):
        if self._proc is None:
            return
        self._stop_event.set()
        self._thread.join()
        try:
            if self._proc.stdin:
                self._proc.stdin.close()
            self._proc.wait(timeout=5.0)
        except Exception:
            self._proc.kill()
        finally:
            self._proc = None
