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
import queue
import subprocess
import threading

import cv2
import numpy as np
import rclpy
from pipeline_interfaces.msg import Control, Frame, Metadata
from rclpy.node import Node
from rclpy.qos import QoSDurabilityPolicy, QoSHistoryPolicy, QoSProfile, QoSReliabilityPolicy
from rclpy.time import Time
from sensor_msgs.msg import Image

CONTROL_TOPIC = 'control'
NEW_FRAME_TOPIC = 'new_frame'
OUT_C_TOPIC = 'out_c'
METADATA_A_TOPIC = 'metadata_a'
# The image now only passes through B and C (A produces metadata, not a stamp).
EXPECTED_STAMP_ORDER = ['postprocessB', 'postprocessC']
DEFAULT_FPS = 30.0
WORKER_POLL_TIMEOUT_S = 0.5
# Logging every frame was fine for the original 5-image demo but at real
# video frame rate it adds enough per-call overhead (console I/O, plus
# rosout's own DDS publish) on the executor thread to slowly starve it —
# only log a sample of frames on the hot path.
LOG_EVERY_N_FRAMES = 30
NEW_FRAME_SUBSCRIBERS_EXPECTED = 2  # postprocessA + postprocessB
DISCOVERY_POLL_INTERVAL_S = 0.2
DISCOVERY_MAX_WAIT_S = 30.0
# postprocessB->postprocessC is a two-hop *serial* worker-thread chain; it
# has a real, finite throughput ceiling regardless of the simulated delay
# setting (message (de)serialization, numpy copies, cv2.putText per hop).
# new_frame has no flow control of its own, so without a cap here manager
# would keep publishing at the video's native rate even once the chain
# falls behind, and the backlog (and therefore latency) grows without
# bound. Pausing sends once too many frames are in flight paces the whole
# pipeline to whatever it can actually sustain instead.
MAX_IN_FLIGHT_FRAMES = 5
# emitted_count is a simple counter, not a per-frame-id ledger, so a frame
# that's lost for good (BEST_EFFORT QoS permits this, especially during
# DDS discovery ramp-up right at startup) leaves a permanent gap: in_flight
# never shrinks back down for it, and enough accumulated losses pin
# in_flight at the cap forever with no way to recover. Expiring a pending
# frame after this long makes a lost frame cost one timeout instead of
# indefinitely eating into the in-flight budget.
PENDING_FRAME_TIMEOUT_S = 2.0

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
        # rosout mirrors every log call onto a DDS topic; at real video
        # frame rate that's meaningful per-call overhead on the same
        # executor thread that has to keep up with incoming frames.
        super().__init__('manager', enable_rosout=False)

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
        # gst-launch's stdin pipe is only ~64KB; writing a full 2.7MB raw
        # frame blocks until gst-launch drains enough of it. Doing that
        # write directly on a ROS callback would stall the whole executor
        # (and therefore every incoming topic) for as long as encoding one
        # frame takes, so a background worker thread owns the actual write
        # -- same pattern postprocessB/C use for their fake-processing sleep.
        self._gst_queue = queue.Queue()
        self._gst_stop_event = threading.Event()
        self._gst_worker_thread = threading.Thread(target=self._gst_worker_loop, daemon=True)
        self._gst_worker_thread.start()

        self.control_pub = self.create_publisher(Control, CONTROL_TOPIC, CONTROL_QOS)
        self.frame_pub = self.create_publisher(Frame, NEW_FRAME_TOPIC, FRAME_QOS)
        self.create_subscription(Frame, OUT_C_TOPIC, self.on_final_frame, FRAME_QOS)
        self.create_subscription(Metadata, METADATA_A_TOPIC, self.on_metadata, FRAME_QOS)

        # Per frame_id: holds whichever of {'image', 'meta'} has arrived so
        # far. A frame is only burned-in and streamed once both are present,
        # since arrival order between the metadata branch (A) and the image
        # chain (B->C) isn't guaranteed.
        self.pending = {}
        self.sent_at = {}  # frame_id -> Time, for expiring stale pending entries
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
        self._estimated_frame_count = estimated_frame_count
        # Wait for postprocessA/B to actually subscribe before sending
        # anything, rather than guessing a fixed delay: a frame published
        # before subscribers exist is silently dropped (default QoS is
        # VOLATILE), and with the in-flight cap below, even ONE dropped
        # frame near the start would stall the whole pipeline forever
        # waiting for it to "complete".
        self._discovery_start = self.get_clock().now()
        self.discovery_timer = self.create_timer(
            DISCOVERY_POLL_INTERVAL_S, self._on_discovery_check)

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

    def _on_discovery_check(self):
        matched = self.frame_pub.get_subscription_count()
        waited_s = (self.get_clock().now() - self._discovery_start).nanoseconds / 1e9
        if matched < NEW_FRAME_SUBSCRIBERS_EXPECTED and waited_s < DISCOVERY_MAX_WAIT_S:
            return  # keep polling
        self.discovery_timer.cancel()
        if matched < NEW_FRAME_SUBSCRIBERS_EXPECTED:
            self.get_logger().warning(
                f'timed out waiting for new_frame subscribers '
                f'({matched}/{NEW_FRAME_SUBSCRIBERS_EXPECTED} after {waited_s:.1f}s); '
                f'starting anyway')
        else:
            self.get_logger().info(f'new_frame subscribers ready after {waited_s:.1f}s')

        control_msg = Control(
            mode=self.mode, command=Control.RUN, total_frames=self._estimated_frame_count)
        self.control_pub.publish(control_msg)
        # One frame per timer tick (not a sleep loop) so return callbacks for
        # early frames are never stuck behind later sends.
        self.send_timer = self.create_timer(self.send_interval_s, self._on_send_timer)

    def _on_send_timer(self):
        self._expire_stale_pending()
        in_flight = self._next_frame_index - self.emitted_count
        if in_flight >= MAX_IN_FLIGHT_FRAMES:
            return  # postprocess chain hasn't caught up yet; wait for the next tick

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
        self.sent_at[frame_id] = self.get_clock().now()
        if frame_id % LOG_EVERY_N_FRAMES == 0:
            self.get_logger().info(f'sent frame_id={frame_id} mode={self.mode}')

        self._next_frame_index += 1

    def _expire_stale_pending(self):
        now = self.get_clock().now()
        for frame_id, sent_time in list(self.sent_at.items()):
            if frame_id in self.pending and (now - sent_time).nanoseconds / 1e9 > PENDING_FRAME_TIMEOUT_S:
                self.get_logger().warning(
                    f'frame_id={frame_id} incomplete after {PENDING_FRAME_TIMEOUT_S}s '
                    f'(likely dropped under best-effort QoS); giving up on it')
                del self.pending[frame_id]
                del self.sent_at[frame_id]
                self.emitted_count += 1

    def on_final_frame(self, msg):
        entry = self.pending.setdefault(msg.frame_id, {})
        if 'image' in entry:
            return  # already handled (or unexpected re-delivery); ignore

        t_recv = self.get_clock().now()
        transport_ms = elapsed_ms(t_recv, msg.hop_stamp)
        ring_total_ms = elapsed_ms(t_recv, msg.origin_stamp)
        ok = list(msg.stamped_by) == EXPECTED_STAMP_ORDER
        if msg.frame_id % LOG_EVERY_N_FRAMES == 0:
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
        if msg.frame_id % LOG_EVERY_N_FRAMES == 0:
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
        self._gst_queue.put(img)

        self.image_results[frame_id] = entry['image_stats']
        self.metadata_results[frame_id] = entry['meta']
        del self.pending[frame_id]
        self.sent_at.pop(frame_id, None)
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

    def _gst_worker_loop(self):
        # Keep draining even after stop is requested, so frames already
        # queued by the time shutdown starts still reach gst-launch.
        while not self._gst_stop_event.is_set() or not self._gst_queue.empty():
            try:
                img = self._gst_queue.get(timeout=WORKER_POLL_TIMEOUT_S)
            except queue.Empty:
                continue
            try:
                self.gst_proc.stdin.write(img.tobytes())
            except (BrokenPipeError, ValueError):
                self.get_logger().warning('gst pipeline stdin closed; frame dropped')

    def stop_gst(self):
        if self.gst_proc is None:
            return
        self._gst_stop_event.set()
        self._gst_worker_thread.join()
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
