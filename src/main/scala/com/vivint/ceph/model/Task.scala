package com.vivint.ceph
package model

import akka.actor.ActorContext
import java.util.UUID
import org.apache.mesos.Protos
import mesosphere.mesos.matcher.ResourceMatcher

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
  taskStatus: Option[Protos.TaskStatus] = None
) {
  if (wantingNewOffer)
    require(heldOffer.isEmpty, "cannot want offer and be holding an offer")
  def readyForOffer =
    wantingNewOffer && heldOffer.isEmpty && offerMatcher.nonEmpty
  lazy val taskId = Task.makeTaskId(role, cluster, id)
  taskStatus.foreach { s =>
    require(s.getTaskId.getValue == taskId, "Critical error - TaskStatus must match generated task state")
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
    if (status.getState == Protos.TaskState.TASK_RUNNING)
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
    } yield ProtoHelpers.newTaskStatus(taskId, slaveId)

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
    s"${role}#${cluster}#${id.toString}"

}
