"""manager: reads a video file and publishes it frame-by-frame in real time
on new_frame, which fans out to two branches:

    manager --new_frame--> postprocessA --metadata_a--> manager   (metadata branch)
    manager --new_frame--> postprocessB --out_b--> postprocessC --out_c--> manager  (image chain)

Set ENABLE_POSTPROCESS_C=0 (read by both this node and postprocessB) to
bypass postprocessC: postprocessB then publishes straight to out_c.

For each frame_id, once BOTH the final stamped image (out_c) and the
metadata (metadata_a) have arrived, the manager burns frame_id and
mean_brightness onto the frame and streams it live over UDP via a
gst-launch-1.0 subprocess (MJPEG-over-RTP, since a raw JPEG frame can
exceed a single UDP datagram). Fully self-contained: no shared code with
the postprocess packages beyond the pipeline_interfaces message
definitions.

This node is deliberately just the ROS wiring + orchestration; the actual
work is split out into: config.py (env/constants), topics.py (topic names
+ QoS), frame_codec.py (message<->image conversions), gst_sink.py (the
streaming subprocess), video_source.py (real-time-paced decode), and
frame_sync.py (image/metadata reconciliation).
"""
import os

import rclpy
from pipeline_interfaces.msg import Control, Frame, Metadata
from rclpy.node import Node

from .config import (
    DISCOVERY_MAX_WAIT_S,
    DISCOVERY_POLL_INTERVAL_S,
    EXPECTED_STAMP_COUNT,
    LOG_EVERY_N_FRAMES,
    MODE_TO_INT,
    NEW_FRAME_SUBSCRIBERS_EXPECTED,
    PENDING_FRAME_TIMEOUT_S,
)
from .frame_codec import burn_metadata_on_frame, elapsed_ms, image_to_msg, msg_to_image
from .frame_sync import FrameSync
from .gst_sink import GstSink
from .topics import (
    CONTROL_QOS,
    CONTROL_TOPIC,
    FRAME_QOS,
    METADATA_A_TOPIC,
    NEW_FRAME_TOPIC,
    OUT_C_TOPIC,
)
from .video_source import PacedVideoSource


class ManagerNode(Node):

    def __init__(self):
        # rosout mirrors every log call onto a DDS topic; at real video
        # frame rate that's meaningful per-call overhead on the same
        # executor thread that has to keep up with incoming frames.
        super().__init__('manager', enable_rosout=False)

        self.mode = os.environ.get('MODE', 'copy')
        self.video_path = os.environ.get(
            'VIDEO_PATH', '/workspace/input/video_720.mp4')
        gst_udp_host = os.environ.get('GST_UDP_HOST', 'host.docker.internal')
        gst_udp_port = int(os.environ.get('GST_UDP_PORT', '5000'))
        self.timeout_s = float(os.environ.get('COLLECT_TIMEOUT_S', '30.0'))

        self.video_source = PacedVideoSource(self.video_path)
        self.gst_sink = GstSink(
            self.video_source.width, self.video_source.height, self.video_source.fps,
            gst_udp_host, gst_udp_port, self.get_logger())
        self.frame_sync = FrameSync(self.get_logger())

        self.control_pub = self.create_publisher(
            Control, CONTROL_TOPIC, CONTROL_QOS)
        self.frame_pub = self.create_publisher(
            Frame, NEW_FRAME_TOPIC, FRAME_QOS)
        self.create_subscription(
            Frame, OUT_C_TOPIC, self.on_final_frame, FRAME_QOS)
        self.create_subscription(
            Metadata, METADATA_A_TOPIC, self.on_metadata, FRAME_QOS)

        self.total_frames = None  # == total_sent, known only once EOF is reached
        self.timeout_timer = None
        self.send_timer = None
        # Runs independently of send_timer (which stops at EOF) so trailing
        # frames near the end of the video still get garbage-collected and
        # _maybe_finish can trigger promptly instead of always waiting out
        # the full COLLECT_TIMEOUT_S fallback.
        self.gc_timer = self.create_timer(
            PENDING_FRAME_TIMEOUT_S / 2, self._expire_stale_pending)

        self.get_logger().info(
            f'manager starting, mode={self.mode}, video={self.video_path} '
            f'{self.video_source.width}x{self.video_source.height}@{self.video_source.fps:.2f}fps '
            f'(~{self.video_source.estimated_frame_count} frames estimated)')
        # Wait for postprocessA/B to actually subscribe before sending
        # anything, rather than guessing a fixed delay: a frame published
        # before subscribers exist is silently dropped (default QoS is
        # VOLATILE), which would just show up as one more dropped frame --
        # harmless on its own, but no reason to waste it right at startup.
        self._discovery_start = self.get_clock().now()
        self.discovery_timer = self.create_timer(
            DISCOVERY_POLL_INTERVAL_S, self._on_discovery_check)

    def _on_discovery_check(self):
        matched = self.frame_pub.get_subscription_count()
        waited_s = (self.get_clock().now() -
                    self._discovery_start).nanoseconds / 1e9
        if matched < NEW_FRAME_SUBSCRIBERS_EXPECTED and waited_s < DISCOVERY_MAX_WAIT_S:
            return  # keep polling
        self.discovery_timer.cancel()
        if matched < NEW_FRAME_SUBSCRIBERS_EXPECTED:
            self.get_logger().warning(
                f'timed out waiting for new_frame subscribers '
                f'({matched}/{NEW_FRAME_SUBSCRIBERS_EXPECTED} after {waited_s:.1f}s); '
                f'starting anyway')
        else:
            self.get_logger().info(
                f'new_frame subscribers ready after {waited_s:.1f}s')

        control_msg = Control(
            mode=self.mode, command=Control.RUN,
            total_frames=self.video_source.estimated_frame_count)
        self.control_pub.publish(control_msg)
        # Start decode and sending together -- decoding is gated until now so
        # the video isn't burned through during the discovery wait.
        self.video_source.start()
        # One frame per timer tick (not a sleep loop) so return callbacks for
        # early frames are never stuck behind later sends.
        self.send_timer = self.create_timer(
            self.video_source.send_interval_s, self._on_send_timer)

    def _on_send_timer(self):
        # Pure consumer: grab the freshest decoded frame (if any) and publish
        # it. Decoding + real-time pacing live on PacedVideoSource; content
        # stays locked to the wall clock there, so falling behind here just
        # means skipping ahead in content, never slowing down to catch up.
        item = self.video_source.take_latest()

        if item is None:
            if self.video_source.eof:          # decode finished and slot drained
                self.send_timer.cancel()
                self.video_source.stop()
                self.total_frames = self.frame_sync.total_sent
                self.get_logger().info(
                    f'end of video reached, sent {self.frame_sync.total_sent} frame(s), '
                    f'dropped {self.frame_sync.dropped_count} for backlog')
                self.timeout_timer = self.create_timer(
                    self.timeout_s, self._on_timeout)
                self._maybe_finish()
            return                              # no fresh frame this tick

        frame_id, img = item

        if self.frame_sync.backlog_full():
            # Downstream hasn't caught up on the frames already sent; skip
            # this one outright rather than queueing it for later -- a live
            # feed should show something close to "now", not something
            # increasingly old.
            self.frame_sync.record_dropped()
            if frame_id % LOG_EVERY_N_FRAMES == 0:
                self.get_logger().info(
                    f'dropped frame_id={frame_id} (backlog full, '
                    f'{self.frame_sync.dropped_count} dropped so far)')
            return

        stamp = self.get_clock().now().to_msg()
        frame_msg = Frame(
            frame_id=frame_id, stamped_by_count=0, mode=MODE_TO_INT[self.mode],
            origin_stamp=stamp, hop_stamp=stamp)
        frame_msg.image_data = image_to_msg(img)
        self.frame_pub.publish(frame_msg)
        self.frame_sync.record_sent(frame_id, self.get_clock().now())
        if frame_id % LOG_EVERY_N_FRAMES == 0:
            self.get_logger().info(
                f'sent frame_id={frame_id} mode={self.mode}')

    def _expire_stale_pending(self):
        now = self.get_clock().now()
        if self.frame_sync.expire_stale(now, PENDING_FRAME_TIMEOUT_S):
            self._maybe_finish()

    def on_final_frame(self, msg):
        if self.frame_sync.has_image(msg.frame_id):
            return  # already handled (or unexpected re-delivery); ignore

        t_recv = self.get_clock().now()
        transport_ms = elapsed_ms(t_recv, msg.hop_stamp)
        ring_total_ms = elapsed_ms(t_recv, msg.origin_stamp)
        ok = msg.stamped_by_count == EXPECTED_STAMP_COUNT
        if msg.frame_id % LOG_EVERY_N_FRAMES == 0:
            self.get_logger().info(
                f'[manager] image frame_id={msg.frame_id} mode={msg.mode} '
                f'transport_ms={transport_ms:.1f} ring_total_ms={ring_total_ms:.1f} '
                f"{'PASS' if ok else 'FAIL'} stamped_by_count={msg.stamped_by_count}")

        stats = {'transport_ms': transport_ms, 'ring_total_ms': ring_total_ms, 'ok': ok}
        ready = self.frame_sync.on_image(msg.frame_id, msg_to_image(msg), stats)
        self._handle_ready(msg.frame_id, ready)

    def on_metadata(self, msg):
        if self.frame_sync.has_meta(msg.frame_id):
            return  # already handled (or unexpected re-delivery); ignore

        t_recv = self.get_clock().now()
        transport_ms = elapsed_ms(t_recv, msg.hop_stamp)
        if msg.frame_id % LOG_EVERY_N_FRAMES == 0:
            self.get_logger().info(
                f'[manager] metadata frame_id={msg.frame_id} mode={msg.mode} '
                f'transport_ms={transport_ms:.1f} {msg.width}x{msg.height} '
                f'mean_brightness={msg.mean_brightness:.1f}')

        meta = {
            'transport_ms': transport_ms,
            'width': msg.width, 'height': msg.height,
            'mean_brightness': msg.mean_brightness,
        }
        ready = self.frame_sync.on_metadata(msg.frame_id, meta)
        self._handle_ready(msg.frame_id, ready)

    def _handle_ready(self, frame_id, ready):
        if ready is None:
            return  # still waiting on the other branch
        image, meta = ready
        img = burn_metadata_on_frame(image, frame_id, meta['mean_brightness'])
        self.gst_sink.send(img)
        self._maybe_finish()

    def _maybe_finish(self):
        if self.frame_sync.is_finished(self.total_frames):
            if self.timeout_timer is not None:
                self.timeout_timer.cancel()
            self.print_summary()

    def print_summary(self):
        image_results = self.frame_sync.image_results
        metadata_results = self.frame_sync.metadata_results

        self.get_logger().info('=== summary ===')
        self.get_logger().info(
            'frame_id | img_transport_ms | ring_total_ms | status | '
            'meta_transport_ms | mean_brightness')
        for frame_id in sorted(image_results):
            r = image_results[frame_id]
            m = metadata_results.get(frame_id, {})
            self.get_logger().info(
                f"{frame_id:>8} | {r['transport_ms']:>16.1f} | "
                f"{r['ring_total_ms']:>13.1f} | {'PASS' if r['ok'] else 'FAIL':>6} | "
                f"{m.get('transport_ms', float('nan')):>17.1f} | "
                f"{m.get('mean_brightness', float('nan')):>15.1f}")
        avg_transport = sum(r['transport_ms']
                            for r in image_results.values()) / len(image_results)
        avg_total = sum(r['ring_total_ms']
                        for r in image_results.values()) / len(image_results)
        self.get_logger().info(
            f'average image transport_ms={avg_transport:.1f} '
            f'average ring_total_ms={avg_total:.1f}')
        self.get_logger().info(
            f'sent={self.frame_sync.total_sent} '
            f'dropped_for_backlog={self.frame_sync.dropped_count} '
            f'completed={len(image_results)}')

        stop_msg = Control(mode=self.mode, command=Control.STOP,
                           total_frames=self.total_frames)
        self.control_pub.publish(stop_msg)

        self.video_source.stop()
        self.gst_sink.stop()
        self.get_logger().info('manager finished all frames, shutting down')
        if rclpy.ok():
            rclpy.shutdown()

    def _on_timeout(self):
        self.timeout_timer.cancel()
        missing = self.frame_sync.missing_frame_ids()
        if missing:
            self.get_logger().warning(
                f'timed out; missing frame_id(s)={missing}')


def main(args=None):
    rclpy.init(args=args)
    node = ManagerNode()
    try:
        rclpy.spin(node)
    except KeyboardInterrupt:
        pass
    finally:
        try:
            node.video_source.stop()
            node.gst_sink.stop()
            node.destroy_node()
        except Exception:
            pass
        if rclpy.ok():
            rclpy.shutdown()


if __name__ == '__main__':
    main()
