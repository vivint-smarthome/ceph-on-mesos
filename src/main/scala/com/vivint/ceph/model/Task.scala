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

  case object TaskStarting extends EnumVal { val id = 0; val name = "TASK_STARTING" }
  case object TaskRunning  extends EnumVal { val id = 1; val name = "TASK_RUNNING" }
  case object TaskFinished extends EnumVal { val id = 2; val name = "TASK_FINISHED" }
  case object TaskFailed   extends EnumVal { val id = 3; val name = "TASK_FAILED" }
  case object TaskKilled   extends EnumVal { val id = 4; val name = "TASK_KILLED" }
  case object TaskLost     extends EnumVal { val id = 5; val name = "TASK_LOST" }
  case object TaskStaging  extends EnumVal { val id = 6; val name = "TASK_STAGING" }
  case object TaskError    extends EnumVal { val id = 7; val name = "TASK_ERROR" }
  case object TaskKilling  extends EnumVal { val id = 8; val name = "TASK_KILLING" }

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
  persistentState: Option[PersistentState] = None,
  behavior: Behavior,
  persistentVersion: Long = 0,
  wantingNewOffer: Boolean = false,
  heldOffer: Option[(PendingOffer, Option[ResourceMatcher.ResourceMatch])] = None,
  offerMatcher: Option[OfferMatchFactory.OfferMatcher] = None,
  taskStatus: Option[TaskStatus] = None
) {
  if (wantingNewOffer)
    require(heldOffer.isEmpty, "cannot want offer and be holding an offer")
  def readyForOffer =
    wantingNewOffer && heldOffer.isEmpty && offerMatcher.nonEmpty
  lazy val taskId = Task.makeTaskId(role, cluster, id)
  taskStatus.foreach { s =>
    require(s.taskId == taskId, "Critical error - TaskStatus must match generated task state")
  }

  lazy val pState = persistentState.getOrElse(
    PersistentState(
      id = id,
      cluster = cluster,
      role = role))

  def peers(p: Iterable[Task]): Iterable[Task] =
    p.filter { peer => (peer.role == this.role) && peer != this }

  def goal = pState.goal
  def lastLaunched = pState.lastLaunched
  def slaveId = pState.slaveId

  @deprecated("move to pState", "")
  def inferPersistedState: PersistentState = pState

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
  def newTask(id: UUID, cluster: String, role: TaskRole.EnumVal, persistentState: Option[PersistentState])(
    implicit actorContext: ActorContext, behaviorSet: BehaviorSet): Task = {

    val taskId = makeTaskId(role = role, cluster = cluster, id = id)
    val taskStatus = for {
      p <- persistentState
      slaveId <- p.slaveId
    } yield TaskStatus(taskId, slaveId, TaskState.TaskLost)

    Task(
      id = id,
      cluster = cluster,
      role = role,
      persistentState = persistentState,
      behavior = behaviorSet.defaultBehaviorFactory(taskId, actorContext),
      taskStatus = taskStatus)
  }

  def forRole(role: TaskRole.EnumVal)(
    implicit actorContext: ActorContext, behaviorSet: BehaviorSet): Task = {
    newTask(
      id = UUID.randomUUID,
      cluster = Constants.DefaultCluster,
      role = role,
      persistentState = None)
  }


  def fromState(state: PersistentState)(
    implicit actorContext: ActorContext, behaviorSet: BehaviorSet): Task = {
    newTask(
      id = state.id,
      cluster = state.cluster,
      role = state.role,
      persistentState = Some(state))
  }
  def makeTaskId(role: TaskRole.EnumVal, cluster: String, id: UUID): String =
    s"${cluster}.${role}.${id.toString}"

}
