"""Environment-driven configuration and pipeline-wide constants for manager_node."""
import os

from pipeline_interfaces.msg import Frame

# Video source is a fixed 1280x720 BGR8 -- these are schema-level facts
# (Frame.image_data is a fixed-size array), not read from the message.
FRAME_WIDTH = 1280
FRAME_HEIGHT = 720
FRAME_DATA_LEN = FRAME_WIDTH * FRAME_HEIGHT * 3

# Must track postprocessB's own ENABLE_POSTPROCESS_C (see
# postprocess_b_node.py): when postprocessC is bypassed, B publishes
# straight to out_c and only one hop stamps the frame before manager sees
# it, instead of two. Reading the same env var here keeps the two in sync
# instead of requiring this to be hand-edited alongside postprocessB's.
ENABLE_POSTPROCESS_C = os.environ.get('ENABLE_POSTPROCESS_C', '1') != '0'
EXPECTED_STAMP_COUNT = 2 if ENABLE_POSTPROCESS_C else 1

MODE_TO_INT = {'copy': Frame.MODE_COPY, 'zero_copy': Frame.MODE_ZERO_COPY}
DEFAULT_FPS = 30.0
WORKER_POLL_TIMEOUT_S = 0.5
# Logging every frame was fine for the original 5-image demo but at real
# video frame rate it adds enough per-call overhead (console I/O, plus
# rosout's own DDS publish) on the executor thread to slowly starve it --
# only log a sample of frames on the hot path.
LOG_EVERY_N_FRAMES = 30
NEW_FRAME_SUBSCRIBERS_EXPECTED = 2  # postprocessA + postprocessB
DISCOVERY_POLL_INTERVAL_S = 0.2
DISCOVERY_MAX_WAIT_S = 30.0
# postprocessB->postprocessC is a two-hop *serial* worker-thread chain; it
# has a real, finite throughput ceiling regardless of the simulated delay
# setting (message (de)serialization, numpy copies, cv2.putText per hop) --
# it can't sustain the video's native rate. This is a LIVE feed, so the
# right response to that is to drop the excess at the source, not to pause
# sending and wait: pausing just turns the same throughput gap into visible
# stutter instead of latency. cap.read() still advances every tick either
# way, so the video stays locked to real elapsed time -- falling behind
# means skipping ahead in content, never slowing down to catch up.
# MAX_PENDING_BACKLOG bounds how many *sent* frames may be unresolved at
# once before new frames stop being published at all (silently skipped,
# not queued for later).
MAX_PENDING_BACKLOG = 6
# Pure garbage collection now, not a latency-critical unblock: a frame
# that's lost for good (BEST_EFFORT QoS permits this) would otherwise sit
# in `pending` forever, permanently inflating the backlog count used above.
# Since nothing waits on this anymore, its only job is keeping that count
# accurate over a long run -- it has no effect on playback smoothness.
PENDING_FRAME_TIMEOUT_S = 1.0
