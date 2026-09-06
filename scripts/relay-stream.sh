#!/usr/bin/env bash
#
# relay-stream.sh — forward the manager's live GStreamer stream (already
# arriving inside this devcontainer on GST_UDP_PORT, pushed there by the
# nested ros2-app stack) onward to a viewer running outside the devcontainer
# entirely, e.g. VLC or gst-launch-1.0 on the Windows host.
#
# The devcontainer only ever *receives* the stream today (manager targets
# host.docker.internal, which resolves to this devcontainer's own netns).
# Nothing outside the devcontainer can see it unless something in here
# re-sends it further out — that's what this script does: it's a dumb
# UDP-datagram relay, so RTP framing/sequencing from the original sender
# passes through untouched.
#
# Usage:
#   ./scripts/relay-stream.sh [destination_host] [destination_port]
#
# Defaults:
#   destination_host = host.docker.internal (Docker Desktop's name for the
#     Windows host, reachable from inside its VM). Note: `getent hosts
#     host.docker.internal` in this environment misleadingly answers with
#     an unroutable IPv6 address; verified with strace that gst-launch-1.0's
#     udpsink actually sends every datagram to the correct IPv4 address
#     (192.168.65.254) regardless, so the name is safe to use here.
#   destination_port = same as the port we're relaying from (GST_UDP_PORT,
#     default 5000)
#
# Requires gst-launch-1.0 in this devcontainer (added to
# .devcontainer/Dockerfile — rebuild the devcontainer if it's not present
# yet).
set -euo pipefail

LISTEN_PORT="${GST_UDP_PORT:-5000}"
DEST_HOST="${1:-host.docker.internal}"
DEST_PORT="${2:-$LISTEN_PORT}"

if ! command -v gst-launch-1.0 >/dev/null 2>&1; then
  echo "error: gst-launch-1.0 not found in this devcontainer." >&2
  echo "       Rebuild the devcontainer (it's now in .devcontainer/Dockerfile)." >&2
  exit 1
fi

echo ">> relaying udp://0.0.0.0:$LISTEN_PORT -> udp://$DEST_HOST:$DEST_PORT"
echo ">> on the receiving end, run:"
echo "   gst-launch-1.0 udpsrc port=$DEST_PORT buffer-size=4194304 caps=\"application/x-rtp,encoding-name=JPEG,payload=26\" ! queue max-size-buffers=0 max-size-bytes=0 max-size-time=500000000 ! rtpjitterbuffer ! rtpjpegdepay ! jpegdec ! videoconvert ! autovideosink"
# buffer-size bumps SO_RCVBUF well past the OS default (~208KB), which is
# too small to absorb one JPEG frame's worth of RTP packets landing in a
# near-instantaneous burst -- the likely cause of the "RTP: missed N
# packets" / "buffers being dropped" symptoms seen end-to-end. The queue
# on this hop adds the same slack before re-sending, so a receive burst
# here doesn't get squeezed back down to nothing before udpsink drains it.
exec gst-launch-1.0 -v udpsrc port="$LISTEN_PORT" buffer-size=4194304 ! \
  queue max-size-buffers=0 max-size-bytes=0 max-size-time=500000000 ! \
  udpsink host="$DEST_HOST" port="$DEST_PORT"
