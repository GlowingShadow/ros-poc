# ros-poc
ROS training program

## ROS2 zero-copy pipeline POC

A 4-container ROS2 (Jazzy) pipeline used to compare normal copy-based
pub/sub against Fast DDS shared-memory zero-copy (Data-Sharing). The manager
publishes each frame once on `new_frame`, which **fans out** to two branches:

```
                       ┌─> postprocessA ──metadata_a──────────────────> manager
manager ──new_frame────┤
                       └─> postprocessB ──out_b──> postprocessC ──out_c──> manager
```

- **Metadata branch** — `postprocessA` reads the frame but does NOT modify it;
  it computes metadata (size + mean brightness) and publishes that on
  `metadata_a` back to the manager.
- **Image chain** — `postprocessB` stamps its name on the frame and forwards it
  on `out_b`; `postprocessC` stamps its name and forwards the result on `out_c`
  back to the manager.

`manager` reads a video file (`input/video_720.mp4` by default — see
`VIDEO_PATH` below) and publishes it frame-by-frame in real time, at the
video's own frame rate. The manager considers a frame **done only when
both** its final image (`out_c`) and its metadata (`metadata_a`) have come
back; it then burns `frame_id` and `mean_brightness` onto the frame (on top
of the `[postprocessB, postprocessC]` name stamps already on it) and streams
it live over UDP via a `gst-launch-1.0` subprocess, instead of saving PNGs to
disk. When the video ends, manager prints a per-frame latency + metadata
summary and shuts down.

Topics (bare names): `new_frame`, `metadata_a`, `out_b`, `out_c`, plus
`control` (manager broadcasts the mode and RUN/STOP to A/B/C).

This pipeline runs in **separate containers** from the main dev-shell
devcontainer (`.devcontainer/`), defined in `docker-compose.ros2.yml`, so it
never affects the normal VS Code devcontainer flow.

Each app is its own independent ROS2 package under `ros2_ws/src/`
(`manager`, `postprocess_a`, `postprocess_b`, `postprocess_c`) with its own
`package.xml`/`setup.py` and its own complete node implementation — as if
written by 4 different people. The only thing they share is the
`pipeline_interfaces` package (`Frame.msg`/`Control.msg`/`Metadata.msg`), which
is the wire contract they all need to agree on to talk to each other; nothing else is
shared, so image conversion/drawing/logging code is duplicated in each
package rather than pulled from a common library. All 4 still build into and
run from the **same Docker image** (`docker/ros2-app/Dockerfile`) — only the
`command:` each container runs (`ros2 run <package> <node>`) differs.

### Usage

Build the shared app image and bring the pipeline up in copy mode:

```bash
docker compose -f docker-compose.ros2.yml up --build
```

Run again in zero-copy mode (uses Fast DDS Data-Sharing over a shared IPC
namespace between the 4 containers):

```bash
docker compose -f docker-compose.ros2.yml down
MODE=zero_copy docker compose -f docker-compose.ros2.yml up --build
```

`manager` exits once it has sent and collected back every frame of the
video; `postprocessA/B/C` keep running (stop the whole stack with
`docker compose -f docker-compose.ros2.yml down`).

### Video streaming

`manager` reads `VIDEO_PATH` (default `/workspace/input/video_720.mp4`,
inside the repo-mounted `/workspace`) and streams the fully-processed result
out over UDP as MJPEG-over-RTP (a raw JPEG frame can be larger than a single
UDP datagram, hence RTP's fragmentation/reassembly) to `GST_UDP_HOST:GST_UDP_PORT`
(defaults `host.docker.internal:5000` — the manager container's Docker
host-gateway, i.e. this devcontainer shell).

Env vars (all optional, set via `docker-compose.ros2.yml` or the shell
environment before `docker compose up`):

| Var | Default | Purpose |
| --- | --- | --- |
| `VIDEO_PATH` | `/workspace/input/video_720.mp4` | Source video read by `manager` |
| `GST_UDP_HOST` | `host.docker.internal` | Where `manager`'s gst-launch sender sends the stream |
| `GST_UDP_PORT` | `5000` | UDP port for the stream |
| `MIN_PROCESSING_DELAY_S` / `MAX_PROCESSING_DELAY_S` | `0.0` / `0.02` | Simulated per-hop postprocess delay — kept near zero so the two-hop B→C chain can keep up with real video frame rate |

To watch the stream, run a receiver directly in the devcontainer shell while
the pipeline is up:

```bash
gst-launch-1.0 udpsrc port=5000 caps="application/x-rtp,encoding-name=JPEG,payload=26" ! \
  rtpjitterbuffer ! rtpjpegdepay ! jpegdec ! videoconvert ! <your-sink-of-choice>
```

Swap `<your-sink-of-choice>` for whatever fits your setup, e.g.
`autovideosink` if you have a display forwarded to this shell, or
`filesink location=/workspace/output/stream.mjpeg` behind a muxer to save it
and watch afterward. To just confirm packets are arriving with no display at
all: `gst-launch-1.0 udpsrc port=5000 ! fakesink -v`.

**Watching from outside the devcontainer** (e.g. VLC or gst-launch-1.0 on the
Windows host): the devcontainer only *receives* the stream by default — it
publishes no ports, and `manager`'s UDP send is unicast to one destination,
so nothing outside can just "tune in". `scripts/relay-stream.sh` re-sends
whatever arrives on `GST_UDP_PORT` (5000) on to a further destination
(default: `host.docker.internal`, i.e. the Windows host, if you're on Docker
Desktop). Run it in the devcontainer shell while the pipeline is up:

```bash
./scripts/relay-stream.sh                 # relay to host.docker.internal:5000
./scripts/relay-stream.sh <host> <port>   # relay somewhere else
```

Then, on the receiving end, run the same `gst-launch-1.0` command shown
above (adjusted for whatever GStreamer/VLC install is available there).

### Verifying the result

- **Visual**: receive the stream (see above) and confirm each frame shows
  the two name stamps `postprocessB`/`postprocessC` (postprocessA does not
  stamp — it only reports metadata) plus a third line burned in by
  `manager` itself: `frame=<id> brightness=<mean_brightness>`.
- **Metadata**: the `manager` log shows a `mean_brightness` and `WxH` per
  frame, reported by `postprocessA` over `metadata_a`.
- **Latency**: compare the `manager` log's final summary table (columns
  `img_transport_ms` / `ring_total_ms` / `meta_transport_ms` per `frame_id`)
  between a `MODE=copy` run and a `MODE=zero_copy` run — the `transport_ms`
  columns isolate pure messaging/transport cost (exclude the random
  fake-processing sleep) and should be lower in `zero_copy` mode.
- **Live topic inspection** (optional, while a run is in progress):
  ```bash
  docker compose -f docker-compose.ros2.yml exec manager ros2 topic echo control
  docker compose -f docker-compose.ros2.yml exec manager ros2 topic hz new_frame
  ```
