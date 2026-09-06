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

`manager` generates a fixed set of sample frames (auto-generated placeholder
images on first run if `sample_images/` is empty — drop your own images in
there beforehand to skip generation) and sends them once. Each `postprocessX`
sleeps for a random duration (fake processing). The manager considers a frame
**done only when both** its final image (`out_c`) and its metadata
(`metadata_a`) have come back; it verifies the image carries the two stamps
`[postprocessB, postprocessC]`, saves it to `output_frames/`, and prints a
per-frame latency + metadata summary.

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

`manager` exits once it has sent and collected back all sample frames;
`postprocessA/B/C` keep running (stop the whole stack with
`docker compose -f docker-compose.ros2.yml down`).

### Verifying the result

- **Visual**: open `output_frames/frame_0_copy.png` … (or `_zero_copy.png`)
  and confirm each image shows the original sample content plus 2 distinct,
  non-overlapping name stamps: `postprocessB`, `postprocessC` (postprocessA
  does not stamp — it only reports metadata).
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
