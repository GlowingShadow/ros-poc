from setuptools import find_packages, setup

package_name = 'manager'

setup(
    name=package_name,
    version='0.1.0',
    packages=find_packages(exclude=['test']),
    data_files=[
        ('share/ament_index/resource_index/packages',
            ['resource/' + package_name]),
        ('share/' + package_name, ['package.xml']),
    ],
    install_requires=['setuptools'],
    zip_safe=True,
    maintainer='romainhoffschir',
    maintainer_email='romainhoffschir@gmail.com',
    description='Generator + collector node for the ROS2 zero-copy pipeline POC',
    license='Apache-2.0',
    tests_require=['pytest'],
    entry_points={
        'console_scripts': [
            'manager_node = manager.manager_node:main',
        ],
    },
)
