"""Parameterized postprocessA/B/C role: receives a frame, sleeps for an
arbitrary duration (fake processing), stamps its name on the frame, and
forwards it to the next hop."""
import os
import time

import rclpy
from pipeline_interfaces.msg import Control, Frame
from rclpy.node import Node
from rclpy.qos import QoSDurabilityPolicy, QoSProfile, QoSReliabilityPolicy

from pipeline_apps import common

CONTROL_QOS = QoSProfile(
    depth=1,
    reliability=QoSReliabilityPolicy.RELIABLE,
    durability=QoSDurabilityPolicy.TRANSIENT_LOCAL,
)


class PostprocessNode(Node):

    def __init__(self):
        role = os.environ.get('NODE_ROLE')
        if role not in common.ROLE_TABLE:
            raise RuntimeError(
                f'NODE_ROLE must be one of {list(common.ROLE_TABLE)}, got {role!r}')
        super().__init__(role)

        self.role = role
        self.mode = os.environ.get('MODE', 'copy')
        route = common.ROLE_TABLE[role]
        self.slot = route['slot']
        self.min_delay_s = float(os.environ.get('MIN_PROCESSING_DELAY_S', '0.1'))
        self.max_delay_s = float(os.environ.get('MAX_PROCESSING_DELAY_S', '0.6'))

        self.create_subscription(Control, common.CONTROL_TOPIC, self.on_control, CONTROL_QOS)
        self.publisher = self.create_publisher(Frame, route['out'], 10)
        self.create_subscription(Frame, route['in'], self.on_frame, 10)

        self.get_logger().info(
            f"{role} ready, mode={self.mode}, in={route['in']}, out={route['out']}")

    def on_control(self, msg):
        self.mode = msg.mode
        command = 'RUN' if msg.command == Control.RUN else 'STOP'
        self.get_logger().info(f'control: mode={msg.mode} command={command}')

    def on_frame(self, msg):
        t_recv = self.get_clock().now()
        img = common.msg_to_image(msg.image)

        delay = common.random_processing_delay(self.min_delay_s, self.max_delay_s)
        time.sleep(delay)

        img = common.stamp_name_on_frame(img, self.role, self.slot)

        out_msg = Frame(
            frame_id=msg.frame_id,
            stamped_by=list(msg.stamped_by) + [self.role],
            mode=msg.mode,
            origin_stamp=msg.origin_stamp,
            hop_stamp=self.get_clock().now().to_msg(),
        )
        out_msg.image = common.image_to_msg(img)
        self.publisher.publish(out_msg)
        t_pub = self.get_clock().now()

        common.log_hop(
            self.get_logger(), self.role, msg.frame_id, msg.mode,
            msg.hop_stamp, msg.origin_stamp, t_recv, t_pub)


def main(args=None):
    rclpy.init(args=args)
    node = PostprocessNode()
    try:
        rclpy.spin(node)
    except KeyboardInterrupt:
        pass
    finally:
        node.destroy_node()
        rclpy.shutdown()


if __name__ == '__main__':
    main()
