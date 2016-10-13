package com.vivint.ceph
package model

import java.util.UUID
import mesosphere.mesos.matcher.ResourceMatcher
import org.apache.mesos.Protos

case class Job(
  id: UUID,
  cluster: String,
  role: JobRole.EnumVal,
  version: Long = 0,
  pState: PersistentState,
  behavior: Behavior,
  persistentVersion: Long = 0,
  purged: Boolean = false,
  wantingNewOffer: Boolean = false,
  heldOffer: Option[(PendingOffer, Option[ResourceMatcher.ResourceMatch])] = None,
  taskState: Option[TaskState.EnumVal] = None
) {
  if (wantingNewOffer)
    require(heldOffer.isEmpty, "cannot want offer and be holding an offer")
  def readyForOffer =
    wantingNewOffer && heldOffer.isEmpty
  def taskId = pState.taskId

  lazy val taskStatus =
    for {
      state <- taskState
      tid <- this.taskId
      sid <- this.slaveId
    } yield TaskStatus(tid, sid, state)

  def peers(p: Iterable[Job]): Iterable[Job] =
    p.filter { peer => (peer.role == this.role) && peer.id != this.id }

  def reservationId = pState.reservationId
  def reservationConfirmed = pState.reservationConfirmed
  def goal = pState.goal
  def lastLaunched = pState.lastLaunched
  def slaveId = pState.slaveId

  def withGoal(goal: Option[RunState.EnumVal]): Job =
    copy(pState = pState.copy(goal = goal))

  /** Given a mesos status, reads the task status, infers and stores IP address if it is avail
    */
  def withTaskStatus(taskStatus: Protos.TaskStatus): Job = {
    import scala.collection.JavaConversions._
    val inferredIp = taskStatus.getContainerStatus.getNetworkInfosList.
      toStream.
      flatMap { _.getIpAddressesList.toStream }.
      map { _.getIpAddress }.
      headOption

    val nextLocation = (inferredIp, pState.location.ipOpt) match {
      case (None, Some(ip)) => pState.location.withIP(ip)
      case _ => pState.location
    }

    copy(
      taskState = Some(TaskState.fromMesos(taskStatus.getState)),
      pState = pState.copy(
        location = nextLocation,
        taskId = Some(taskStatus.getTaskId.getValue),
        slaveId = Some(taskStatus.getSlaveId.getValue)))
  }

  /** If task is running
    */
  def runningState: Option[RunState.EnumVal] = for {
    status <- taskStatus
    launched <- pState.lastLaunched
    if (status.state == TaskState.TaskRunning)
  } yield {
    launched
  }
}

object Job {
  def newTask(id: UUID, cluster: String, role: JobRole.EnumVal, pState: PersistentState,
    defaultBehavior: JobRole.EnumVal => Behavior): Job = {

    Job(
      id = id,
      cluster = cluster,
      role = role,
      pState = pState,
      behavior = defaultBehavior(role),
      persistentVersion = 0,
      version = 1
    )
  }

  def forRole(role: JobRole.EnumVal, defaultBehavior: JobRole.EnumVal => Behavior): Job = {
    val id = UUID.randomUUID
    newTask(
      id = id,
      cluster = Constants.DefaultCluster,
      role = role,
      pState = PersistentState(id = id, cluster = Constants.DefaultCluster, role = role),
      defaultBehavior = defaultBehavior)
  }


  def fromState(state: PersistentState, defaultBehavior: JobRole.EnumVal => Behavior): Job = {
    newTask(
      id = state.id,
      cluster = state.cluster,
      role = state.role,
      pState = state,
      defaultBehavior = defaultBehavior
    )
  }
  def makeTaskId(role: JobRole.EnumVal, cluster: String): String =
    s"${cluster}.${role}.${UUID.randomUUID.toString}"

}
