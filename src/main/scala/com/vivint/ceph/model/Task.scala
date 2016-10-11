package com.vivint.ceph
package model

import akka.actor.ActorContext
import java.util.UUID
import mesosphere.mesos.matcher.ResourceMatcher
import org.apache.mesos.Protos

object TaskState extends lib.Enum {
  sealed trait EnumVal extends Value {
    val id: Int
  }

  sealed trait Active extends EnumVal
  sealed trait Terminal extends EnumVal

  case object TaskStarting extends Active   { val id = 0; val name = "TASK_STARTING" }
  case object TaskStaging  extends Active   { val id = 6; val name = "TASK_STAGING" }
  case object TaskRunning  extends Active   { val id = 1; val name = "TASK_RUNNING" }
  case object TaskKilling  extends Active   { val id = 8; val name = "TASK_KILLING" }
  case object TaskFinished extends Terminal { val id = 2; val name = "TASK_FINISHED" }
  case object TaskFailed   extends Terminal { val id = 3; val name = "TASK_FAILED" }
  case object TaskKilled   extends Terminal { val id = 4; val name = "TASK_Ktate.LLED" }
  case object TaskError    extends Terminal { val id = 7; val name = "TASK_ERROR" }
  case object TaskLost     extends EnumVal  { val id = 5; val name = "TASK_LOST" }

  def values = Vector(TaskError, TaskFailed, TaskFinished, TaskKilled, TaskKilling, TaskLost, TaskRunning, TaskStaging,
    TaskStarting)

  def valuesById: Map[Int, EnumVal] =
    values.groupBy(_.id).map { case (id, Seq(value)) => id -> value }
}

case class TaskStatus(taskId: String, slaveId: String, state: TaskState.EnumVal) {
  def toMesos: Protos.TaskStatus = {
    ProtoHelpers.newTaskStatus(taskId, slaveId, Protos.TaskState.valueOf(state.id))
  }
}

object TaskStatus extends ((String, String, TaskState.EnumVal) => TaskStatus) {
  def fromMesos(p: Protos.TaskStatus) = {
    TaskStatus(
      p.getTaskId.getValue,
      p.getSlaveId.getValue,
      TaskState.valuesById(p.getState.getNumber))
  }
}

case class Task(
  id: UUID,
  cluster: String,
  role: TaskRole.EnumVal,
  version: Long = 0,
  pState: PersistentState,
  behavior: Behavior,
  persistentVersion: Long = 0,
  purged: Boolean = false,
  wantingNewOffer: Boolean = false,
  heldOffer: Option[(PendingOffer, Option[ResourceMatcher.ResourceMatch])] = None,
  taskStatus: Option[TaskStatus] = None
) {
  if (wantingNewOffer)
    require(heldOffer.isEmpty, "cannot want offer and be holding an offer")
  def readyForOffer =
    wantingNewOffer && heldOffer.isEmpty
  lazy val taskId = Task.makeTaskId(role, cluster, id)
  taskStatus.foreach { s =>
    require(s.taskId == taskId, "Critical error - TaskStatus must match generated task state")
  }

  def peers(p: Iterable[Task]): Iterable[Task] =
    p.filter { peer => (peer.role == this.role) && peer.id != this.id }

  def reservationConfirmed = pState.reservationConfirmed
  def goal = pState.goal
  def lastLaunched = pState.lastLaunched
  def slaveId = pState.slaveId

  def withGoal(goal: Option[RunState.EnumVal]): Task =
    copy(pState = pState.copy(goal = goal))

  /** Given a mesos status, reads the task status, infers and stores IP address if it is avail
    */
  def withTaskStatus(taskStatus: Protos.TaskStatus): Task = {
    import scala.collection.JavaConversions._
    val inferredIp = taskStatus.getContainerStatus.getNetworkInfosList.
      toStream.
      flatMap { _.getIpAddressesList.toStream }.
      map { _.getIpAddress }.
      headOption

    val nextPState = (pState.location.ipOpt, inferredIp) match {
      case (None, Some(ip)) => pState.copy(location = pState.location.withIP(ip))
      case _ => pState
    }

    copy(
      taskStatus = Some(TaskStatus.fromMesos(taskStatus)),
      pState = nextPState)
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

object Task {
  def newTask(id: UUID, cluster: String, role: TaskRole.EnumVal, pState: PersistentState,
    defaultBehavior: TaskRole.EnumVal => Behavior): Task = {

    val taskId = makeTaskId(role = role, cluster = cluster, id = id)
    val taskStatus = for {
      slaveId <- pState.slaveId
    } yield TaskStatus(taskId, slaveId, TaskState.TaskLost)

    Task(
      id = id,
      cluster = cluster,
      role = role,
      pState = pState,
      behavior = defaultBehavior(role),
      taskStatus = taskStatus,
      persistentVersion = 0,
      version = 1
    )
  }

  def forRole(role: TaskRole.EnumVal, defaultBehavior: TaskRole.EnumVal => Behavior): Task = {
    val id = UUID.randomUUID
    newTask(
      id = id,
      cluster = Constants.DefaultCluster,
      role = role,
      pState = PersistentState(id = id, cluster = Constants.DefaultCluster, role = role),
      defaultBehavior = defaultBehavior)
  }


  def fromState(state: PersistentState, defaultBehavior: TaskRole.EnumVal => Behavior): Task = {
    newTask(
      id = state.id,
      cluster = state.cluster,
      role = state.role,
      pState = state,
      defaultBehavior = defaultBehavior
    )
  }
  def makeTaskId(role: TaskRole.EnumVal, cluster: String, id: UUID): String =
    s"${cluster}.${role}.${id.toString}"

}
