# SPDX-FileCopyrightText: 2022-2023, G.A. vd. Hoorn
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import sys
import time
import math
import threading
import asyncio 

import rclpy
from rclpy.action import ActionServer, CancelResponse, GoalResponse
from rclpy.duration import Duration
from rclpy.time import Time

from control_msgs.action import FollowJointTrajectory
from trajectory_msgs.msg import JointTrajectoryPoint
from motoros2_interfaces.msg import QueueResultEnum
from motoros2_interfaces.srv import QueueTrajPoint
from industrial_msgs.msg import RobotStatus, TriState, RobotMode
from sensor_msgs.msg import JointState


MAX_RETRIES_PARAM = "max_retries"
BUSY_WAIT_TIME_PARAM = "busy_wait_time"
QUEUE_POINT_RESPONSE_WAIT_TIME_PARAM = "queue_point_response_wait_time"
CONVERGENCE_THRESHOLD_PARAM = "convergence_threshold"
ABORT_ON_TIMEOUT_PARAM = "abort_on_timeout"

MAX_RETRIES_DEFAULT = 20
BUSY_WAIT_TIME_DEFAULT = 0.05
QUEUE_POINT_RESPONSE_WAIT_TIME_DEFAULT = 0.1
CONVERGENCE_THRESHOLD_DEFAULT = 0.01
ABORT_ON_TIMEOUT_DEFAULT = True

class PointQueueProxy:
    def __init__(self, node):
        self._node = node
        self._logger = self._node.get_logger()
        self._logger.info("PointQueueProxy: initialising ..")

        self._goal_handle = None
        self._goal_lock = threading.Lock()

        # Declare ROS parameters
        self._node.declare_parameter(MAX_RETRIES_PARAM, MAX_RETRIES_DEFAULT)
        self._node.declare_parameter(BUSY_WAIT_TIME_PARAM, BUSY_WAIT_TIME_DEFAULT)
        self._node.declare_parameter(QUEUE_POINT_RESPONSE_WAIT_TIME_PARAM, QUEUE_POINT_RESPONSE_WAIT_TIME_DEFAULT)
        self._node.declare_parameter(CONVERGENCE_THRESHOLD_PARAM, CONVERGENCE_THRESHOLD_DEFAULT)
        self._node.declare_parameter(ABORT_ON_TIMEOUT_PARAM, ABORT_ON_TIMEOUT_DEFAULT)

        # maximum nr of retries per traj pt
        try:
            self._max_retries = int(self._node.get_parameter(MAX_RETRIES_PARAM).value)
        except:
            self._logger.warning(f"Failed to load {MAX_RETRIES_PARAM} parameter, " 
                                 f"defaulting to {MAX_RETRIES_DEFAULT}")
        # seconds: how long to wait between (re)submissions
        try:
            self._busy_wait_time = float(self._node.get_parameter(BUSY_WAIT_TIME_PARAM).value)
        except:
            self._logger.warning(f"Failed to load {BUSY_WAIT_TIME_PARAM} parameter, " 
                                 f"defaulting to {BUSY_WAIT_TIME_DEFAULT}")
        # seconds: how long to wait for the point queue server to respond
        try:
            self._point_queue_response_wait_time = float(self._node.get_parameter(QUEUE_POINT_RESPONSE_WAIT_TIME_PARAM).value)
        except:
            self._logger.warning(f"Failed to load {QUEUE_POINT_RESPONSE_WAIT_TIME_PARAM} parameter, " 
                                 f"defaulting to {QUEUE_POINT_RESPONSE_WAIT_TIME_DEFAULT}")
        # radians: total joint distance, not per-joint
        try:
            self._convergence_threshold = float(self._node.get_parameter(CONVERGENCE_THRESHOLD_PARAM).value)
        except:
            self._logger.warning(f"Failed to load {CONVERGENCE_THRESHOLD_PARAM} parameter, " 
                                 f"defaulting to {CONVERGENCE_THRESHOLD_DEFAULT}")
        # true/false: whether timeout is enforced
        try:
            self._abort_on_timeout = bool(self._node.get_parameter(ABORT_ON_TIMEOUT_PARAM).value)
        except:
            self._logger.warning(f"Failed to load {ABORT_ON_TIMEOUT_PARAM} parameter, " 
                                 f"defaulting to {ABORT_ON_TIMEOUT_DEFAULT}")

        original_joint_states_topic: str = 'joint_states'
        original_robot_status_topic: str = 'robot_status'
        original_fjt_namespace: str = 'joint_trajectory_controller'
        original_fjt_name: str = 'follow_joint_trajectory'
        original_queue_pt_srv: str = 'queue_traj_point'
        self._joint_states_topic = self._node.resolve_service_name(original_joint_states_topic)
        self._robot_status_topic = self._node.resolve_service_name(original_robot_status_topic)
        self._fjt_namespace: str = self._node.resolve_service_name(original_fjt_namespace)
        self._fjt_name: str = self._node.resolve_service_name(original_fjt_name)
        self._queue_pt_srv: str = self._node.resolve_service_name(original_queue_pt_srv)

        # first the service client, as there's no point in continuing if the
        # server is not available
        self._cbg_svc = rclpy.callback_groups.MutuallyExclusiveCallbackGroup()
        self._queue_pt_client = self._node.create_client(
            QueueTrajPoint, self._queue_pt_srv, callback_group=self._cbg_svc)
        while not self._queue_pt_client.wait_for_service(timeout_sec=5.0):
            self._logger.info(f'Waiting for queue_traj_point server... ({self._queue_pt_srv})')

        fjt_fully_qualified_name = f'{self._fjt_namespace}{self._fjt_name}'
        self._logger.info(f"Starting action server on '{fjt_fully_qualified_name}'")
        self._action_server = ActionServer(
            self._node, FollowJointTrajectory,
            fjt_fully_qualified_name,
            goal_callback=self.fjt_goal_callback,
            cancel_callback=self.fjt_cancel_callback,
            execute_callback=self.fjt_execute_callback,
            handle_accepted_callback=self.fjt_handle_accepted_callback,
            callback_group=rclpy.callback_groups.ReentrantCallbackGroup()
            )

        # MotoROS2 might be using 'sensor_data' profile or 'default'.
        # Use 'sensor_data' here, as it should be compatible with both
        self._sub_js = self._node.create_subscription(
            JointState, self._joint_states_topic, self._js_callback,
            qos_profile=rclpy.qos.QoSPresetProfiles.get_from_short_key('sensor_data'),
            callback_group=rclpy.callback_groups.MutuallyExclusiveCallbackGroup())
        
        self._sub_rs = self._node.create_subscription(
            RobotStatus, self._robot_status_topic, self._rs_callback,
            qos_profile=rclpy.qos.QoSPresetProfiles.get_from_short_key('sensor_data'),
            callback_group=rclpy.callback_groups.MutuallyExclusiveCallbackGroup())

        # stores last message we received from controller
        self._latest_joint_states: JointState = None
        self._latest_joint_states_lock = threading.Lock()

        self._latest_robot_status: RobotStatus = None
        self._latest_robot_status_lock = threading.Lock()

        self._logger.info("PointQueueProxy: initialisation complete")


    def destroy(self):
        self._action_server.destroy()
        super().destroy_node()


    def fjt_cancel_callback(self, goal):
        self._logger.warning("Received cancel request")
        return CancelResponse.ACCEPT
    

    def fjt_handle_accepted_callback(self, goal_handle):
        with self._goal_lock:
            if self._goal_handle is not None and self._goal_handle.is_active:
                self._logger.warning("Aborting previous goal")
                self._goal_handle.abort()
            self._goal_handle = goal_handle
        goal_handle.execute()
        

    async def _queue_point(self, joint_names: list[str], pt: JointTrajectoryPoint, max_retries: int = 0):
        self._logger.debug(f"attempting to queue pt (max_retries: {max_retries})")

        attempts: int = 0
        result_code: int = -1
        req = QueueTrajPoint.Request(joint_names=joint_names, point=pt)

        # TODO: ugly ternary
        while rclpy.ok() and ((attempts < max_retries) if max_retries else True):
            self._logger.debug(f"queuing pt (attempt: {attempts})")

            # Right now the calling function aborts immediately if the action server does not respond in time
            request_future = self._queue_pt_client.call_async(req)
            try:
                response = await asyncio.wait_for(request_future, self._point_queue_response_wait_time)
            except asyncio.TimeoutError:
                self._logger.error("The queue point server did not respond in time")
                result_code = -1
                break

            # only if we receive a BUSY response we try again. Anything else
            # is something only the caller can handle (including OK)
            result_code = response.result_code.value
            if result_code == QueueResultEnum.BUSY:
                self._logger.debug(
                    f"Busy (attempt: {attempts}). Trying again later.", throttle_duration_sec=1.0)
                attempts += 1
                time.sleep(self._busy_wait_time)
                continue

            if result_code == QueueResultEnum.SUCCESS:
                self._logger.debug(
                    f"queue server accepted point: '{response.message}' ({result_code})")
                break

            # anything else is an error, so report
            self._logger.error(
                f"queue server returned error: '{response.message}' ({result_code})")
            break

        # either -1, or one of the values from QueueResultEnum
        return result_code


    def _js_callback(self, msg):
        with self._latest_joint_states_lock:
            self._latest_joint_states = msg

        with self._goal_lock:
            if self._goal_handle is None or not self._goal_handle.is_active:
                return

        fmsg = FollowJointTrajectory.Feedback()

        # populate fields using latest JointState information we received, as
        # an approximation of the feedback MotoROS2 would send back during the
        # execution of an FJT goal
        fmsg.header.stamp = msg.header.stamp
        fmsg.joint_names = msg.name
        fmsg.actual.positions = msg.position
        fmsg.actual.velocities = msg.velocity
        fmsg.actual.effort = msg.effort

        with self._goal_lock:
            self._goal_handle.publish_feedback(fmsg)


    def _joint_distance(self, d0: dict[str, float], d1: dict[str, float]) -> float:
        # assumptions: d1 contains all keys d0 contains
        assert len(d0) == len(d1)
        return math.fsum([abs(val - d1[name]) for name, val in d0.items()])

    def _rs_callback(self, msg):
        with self._latest_robot_status_lock:
            self._latest_robot_status = msg

        with self._goal_lock:
            if self._goal_handle is not None and self._goal_handle.is_active:
                if msg.e_stopped.val == TriState.TRUE:
                    self._logger.error("The E-Stop was activated. Aborting goal")
                    self._goal_handle.abort()
                elif msg.in_error.val == TriState.TRUE:
                    self._logger.error(f"The controller is in an error state. Aborting goal")
                    self._goal_handle.abort()
                elif msg.drives_powered.val == TriState.FALSE:
                    self._logger.error("The servos are not powered on. Call the /start_point_queue_mode service. Aborting goal")
                    self._goal_handle.abort()
                elif msg.mode.val != RobotMode.AUTO:
                    self._logger.error("The pendant is in teach mode. Change to remote mode. Aborting goal")
                    self._goal_handle.abort()
                elif msg.motion_possible.val == TriState.FALSE:
                    self._logger.error("Motion is no longer possible. Aborting goal")
                    self._goal_handle.abort()


    def fjt_goal_callback(self, goal):
        self._logger.debug('fjt callback: entry')

        traj = goal.trajectory
        points = traj.points

        self._logger.info(f"received goal with {len(points)} traj pts")

        # checks
        with self._latest_joint_states_lock:
            if not self._latest_joint_states:
                error_string = "waiting for (initial) joint_states message from controller"
                self._logger.error(error_string)
                return GoalResponse.REJECT
        with self._latest_robot_status_lock:
            if not self._latest_robot_status:
                error_string = "waiting for (initial) robot_status message from controller"
                self._logger.error(error_string)
                return GoalResponse.REJECT
            elif self._latest_robot_status.e_stopped.val == TriState.TRUE:
                self._logger.error("The E-Stop is active. Rejecting goal")
                return GoalResponse.REJECT
            elif self._latest_robot_status.in_error.val == TriState.TRUE:
                self._logger.error(f"The controller is in an error state. Rejecting goal")
                return GoalResponse.REJECT
            elif self._latest_robot_status.drives_powered.val == TriState.FALSE:
                self._logger.error("The servos are not powered on. Call the /start_point_queue_mode service. Rejecting goal")
                return GoalResponse.REJECT
            elif self._latest_robot_status.mode.val != RobotMode.AUTO:
                self._logger.error("The pendant is in teach mode. Change to remote mode. Rejecting goal")
                return GoalResponse.REJECT
            elif self._latest_robot_status.motion_possible.val == TriState.FALSE:
                self._logger.error("Motion is not longer possible right now. Rejecting goal")
                return GoalResponse.REJECT
            
        if len(points) == 0:
            error_string = "Empty trajectory"
            with self._goal_lock:
                if self._goal_handle is not None and self._goal_handle.is_active:
                    self._goal_handle.abort()
            self._logger.error(error_string)
            return GoalResponse.REJECT

        if len(traj.joint_names) == 0:
            error_string = "no joint names, can't continue"
            self._logger.error(error_string)
            return GoalResponse.REJECT
        
        # arbitrary, but there aren't (m)any Motoman robots with less than
        # four joints, especially not ones supported by MotoROS2
        if len(traj.joint_names) < 4:
            self._logger.warning("less than 4 joint names")

        # TODO: we could/should also check whether joint names in the goal
        # correspond to the MotoROS2 configured joint names, but we have no
        # way of accessing MotoROS2's configuration at the moment.
        # (could potentially sample 'joint_states' topic and use those names)
        self._logger.info("Accepting goal")
        return GoalResponse.ACCEPT

    async def fjt_execute_callback(self, goal_handle):

        self._logger.info("Executing goal")

        traj = goal_handle.request.trajectory
        points = traj.points
        points_sent: int = 0

        last_traj_point = traj.points[-1]
        fjt_requested_duration_msg = last_traj_point.time_from_start
        fjt_requested_duration = Duration.from_msg(fjt_requested_duration_msg)
        goal_time_tolerance_msg = goal_handle.request.goal_time_tolerance
        goal_time_tolerance = Duration.from_msg(goal_time_tolerance_msg)
        
        start_time = Time.from_msg(traj.header.stamp)
        if(start_time.nanoseconds == 0):
            start_time = self._node._clock.now()

        # This order of operations is needed because for some reason, durations can be added to times, but not to each other
        # This is fixed in rolling, but is the case in jazzy and before.
        deadline = (start_time + fjt_requested_duration) + goal_time_tolerance

        # we're going to process the goal, so relay JointStates published
        # by MotoROS2 as FollowJointTrajectory_Feedback

        # iterate over all points, starting with the first. Convert each point
        # to a queue request, then send it off. If not success, repeat until
        # we've reached our time-out value.
        while rclpy.ok() and (points_sent < len(points)):
            self._logger.debug(f"attempting to queue pt {points_sent}")

            # TODO: check whether js watchdog has bitten and cancel/abort goal ourselves

            with self._goal_lock:
                if not goal_handle.is_active:
                    self._logger.error("The goal is no longer active. No longer queueing points")
                    return FollowJointTrajectory.Result(
                        # TODO: use MotoROS2 error reporting method
                        error_code=FollowJointTrajectory.Result.INVALID_GOAL,
                        error_string="Goal aborted") 
                elif goal_handle.is_cancel_requested:
                    goal_handle.canceled()
                    self._logger.error("The goal was cancelled. No longer queueing points")
                    return FollowJointTrajectory.Result(
                        # TODO: use MotoROS2 error reporting method
                        error_code=FollowJointTrajectory.Result.INVALID_GOAL,
                        error_string="Goal cancelled") 
                elif self._abort_on_timeout and self._node._clock.now() > deadline:
                    goal_handle.abort()
                    sec, nsec = deadline.seconds_nanoseconds()
                    self._logger.error(f"Aborting goal: Timeout reached before sending all points -- deadline: {sec}.{nsec}")
                    return FollowJointTrajectory.Result(
                            # TODO: use MotoROS2 error reporting method
                            error_code=FollowJointTrajectory.Result.GOAL_TOLERANCE_VIOLATED,
                            error_string="Goal aborted") 

            pt = points[points_sent]
            result = asyncio.run(self._queue_point(
                joint_names=traj.joint_names, pt=pt, max_retries=self._max_retries)
            )

            # if this is an error, or still BUSY, something is wrong. Abort
            # the goal and report error
            if result != QueueResultEnum.SUCCESS:
                error_string = (f"failed to queue pt {points_sent}, aborting goal "
                                f"(queue server reported: {result})")
                self._logger.error(error_string)
                with self._goal_lock:
                    if self._goal_handle is not None and self._goal_handle.is_active:
                        goal_handle.abort()
                return FollowJointTrajectory.Result(
                    # TODO: use MotoROS2 error reporting method
                    error_code=FollowJointTrajectory.Result.INVALID_GOAL,
                    error_string=error_string)

            self._logger.info(f"pt {points_sent} queued")

            # next pt
            points_sent += 1

        # done
        self._logger.info("queued all points")

        # now we wait until MotoROS2 reports it has reached the final traj pt.
        # We do that by comparing the current JointStates against the final
        # trajectory point in the trajectory submitted as part of the goal.
        # As soon as the distance is below the threshold, we assume the traj
        # has completely executed
        #
        # NOTE: this approach suffers from the exact same problems as the
        # FJT action server in industrial_robot_client (looping traj, etc)
        self._logger.info(
            "waiting for robot to reach final traj pt "
            f"(threshold: {self._convergence_threshold} rad)")

        last_traj_dict = dict(zip(traj.joint_names, points[-1].positions))
        rate = self._node.create_rate(30.0)
        while rclpy.ok():
            with self._goal_lock:
                if not goal_handle.is_active:
                    self._logger.error("The goal is no longer active. No longer waiting for convergence")
                    return FollowJointTrajectory.Result(
                        # TODO: use MotoROS2 error reporting method
                        error_code=FollowJointTrajectory.Result.INVALID_GOAL,
                        error_string="Goal aborted") 
                elif goal_handle.is_cancel_requested:
                    goal_handle.canceled()
                    self._logger.error("The goal was cancelled. No longer waiting for convergence")
                    return FollowJointTrajectory.Result(
                        # TODO: use MotoROS2 error reporting method
                        error_code=FollowJointTrajectory.Result.INVALID_GOAL,
                        error_string="Goal cancelled") 
                elif self._abort_on_timeout and self._node._clock.now() > deadline:
                    goal_handle.abort()
                    sec, nsec = deadline.seconds_nanoseconds()
                    self._logger.error(f"Aborting goal: Timeout reached before convergence -- deadline: {sec}.{nsec}")
                    return FollowJointTrajectory.Result(
                            # TODO: use MotoROS2 error reporting method
                            error_code=FollowJointTrajectory.Result.GOAL_TOLERANCE_VIOLATED,
                            error_string="Goal aborted") 
            with self._latest_joint_states_lock:
                js_dict = dict(zip(
                    self._latest_joint_states.name, self._latest_joint_states.position))
            dist = self._joint_distance(last_traj_dict, js_dict)
            self._logger.debug(
                f"remaining distance: {dist:.4f}", throttle_duration_sec=1)
            if dist <= self._convergence_threshold:
                self._logger.info(
                    f"reached final traj pt (distance: {dist:.4f})")
                break
            rate.sleep()

        with self._goal_lock:
            if not goal_handle.is_active:
                self._logger().error('The goal is no longer active. Something went wrong after convergence')
                return FollowJointTrajectory.Result(
                        # TODO: use MotoROS2 error reporting method
                        error_code=FollowJointTrajectory.Result.INVALID_GOAL,
                        error_string="Goal aborted") 
            goal_handle.succeed()
        result = FollowJointTrajectory.Result(
            error_code=FollowJointTrajectory.Result.SUCCESSFUL,
            error_string="")

        self._logger.debug('fjt callback: exit')
        return result


def main():
    rclpy.init(args=sys.argv)
    node = rclpy.node.Node('motoros2_fjt_pt_queue_proxy')
    server = PointQueueProxy(node)

    executor = rclpy.executors.MultiThreadedExecutor()
    executor.add_node(node)
    executor.spin()


if __name__ == '__main__':
    main()
