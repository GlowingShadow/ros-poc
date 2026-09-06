#!/bin/bash
set -e

source /opt/ros/jazzy/setup.bash

case "${MODE:-copy}" in
  zero_copy)
    export FASTRTPS_DEFAULT_PROFILES_FILE=/ros2_ws/fastdds_profiles_zero_copy.xml
    ;;
  copy)
    export FASTRTPS_DEFAULT_PROFILES_FILE=/ros2_ws/fastdds_profiles_copy.xml
    ;;
  *)
    echo "entrypoint.sh: unknown MODE '${MODE}', expected 'copy' or 'zero_copy'" >&2
    exit 1
    ;;
esac
export RMW_IMPLEMENTATION="${RMW_IMPLEMENTATION:-rmw_fastrtps_cpp}"
export RMW_FASTRTPS_USE_QOS_FROM_XML=1

cd /workspace/ros2_ws
if [ ! -f install/setup.bash ] || [ "${FORCE_REBUILD:-0}" = "1" ]; then
  echo "entrypoint.sh: building ros2_ws (colcon build --symlink-install)..."
  colcon build --symlink-install
fi
source install/setup.bash

exec "$@"
