"""Topic names and QoS profiles for the manager <-> postprocess pipeline.

Every node touching new_frame/out_b/out_c/metadata_a must use FRAME_QOS,
since a reliable subscriber can't match a best-effort publisher.
"""
from rclpy.qos import QoSDurabilityPolicy, QoSHistoryPolicy, QoSProfile, QoSReliabilityPolicy

CONTROL_TOPIC = 'control'
NEW_FRAME_TOPIC = 'new_frame'
OUT_C_TOPIC = 'out_c'
METADATA_A_TOPIC = 'metadata_a'

CONTROL_QOS = QoSProfile(
    depth=1,
    reliability=QoSReliabilityPolicy.RELIABLE,
    durability=QoSDurabilityPolicy.TRANSIENT_LOCAL,
)
# Best-effort/volatile for the per-frame data: at real video frame rate, a
# reliable publisher's retransmit/ack bookkeeping on repeated 2.7MB messages
# adds overhead this pipeline doesn't need -- dropping an occasional frame is
# preferable to blocking on it.
FRAME_QOS = QoSProfile(
    depth=10,
    history=QoSHistoryPolicy.KEEP_LAST,
    reliability=QoSReliabilityPolicy.BEST_EFFORT,
    durability=QoSDurabilityPolicy.VOLATILE,
)
