FROM ros:jazzy-ros-base

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        python3-opencv \
        python3-colcon-common-extensions && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

WORKDIR /workspace

COPY entrypoint.sh /entrypoint.sh
COPY fastdds_profiles_copy.xml /ros2_ws/fastdds_profiles_copy.xml
COPY fastdds_profiles_zero_copy.xml /ros2_ws/fastdds_profiles_zero_copy.xml
RUN chmod +x /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
CMD ["ros2", "run", "pipeline_apps", "manager_node"]
