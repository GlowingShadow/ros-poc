# ros-poc
ROS training program

## ROS2 zero-copy pipeline POC

A 4-container ROS2 (Jazzy) ring pipeline used to compare normal copy-based
pub/sub against Fast DDS shared-memory zero-copy (Data-Sharing):

```
manager --frame_to_a--> postprocessA --frame_to_b--> postprocessB --frame_to_c--> postprocessC --frame_from_c--> manager
```

`manager` generates a fixed set of sample frames (auto-generated placeholder
images on first run if `sample_images/` is empty — drop your own images in
there beforehand to skip generation) and sends them once through the ring.
Each `postprocessX` sleeps for a random duration (fake processing) and stamps
its name onto the frame before forwarding it. `manager` collects the frames
that come back, verifies all 3 stamps are present, saves them to
`output_frames/`, and prints a per-frame latency summary.

This pipeline runs in **separate containers** from the main dev-shell
devcontainer (`.devcontainer/`), defined in `docker-compose.ros2.yml`, so it
never affects the normal VS Code devcontainer flow.

Each app is its own independent ROS2 package under `ros2_ws/src/`
(`manager`, `postprocess_a`, `postprocess_b`, `postprocess_c`) with its own
`package.xml`/`setup.py` and its own complete node implementation — as if
written by 4 different people. The only thing they share is the
`pipeline_interfaces` package (`Frame.msg`/`Control.msg`), which is the wire
contract they all need to agree on to talk to each other; nothing else is
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
  and confirm each image shows the original sample content plus 3 distinct,
  non-overlapping name stamps: `postprocessA`, `postprocessB`, `postprocessC`.
- **Latency**: compare the `manager` log's final summary table (columns
  `transport_ms` / `ring_total_ms` per `frame_id`) between a `MODE=copy` run
  and a `MODE=zero_copy` run — `transport_ms` isolates pure
  messaging/transport cost (excludes the random fake-processing sleep) and
  should be lower in `zero_copy` mode.
- **Live topic inspection** (optional, while a run is in progress):
  ```bash
  docker compose -f docker-compose.ros2.yml exec manager ros2 topic echo /pipeline/control
  docker compose -f docker-compose.ros2.yml exec manager ros2 topic hz /pipeline/frame_to_a
  ```
