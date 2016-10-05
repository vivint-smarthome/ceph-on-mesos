package org.vivint.ceph
package model

import akka.actor.ActorContext
import java.util.UUID
import org.apache.mesos.Protos._
import mesosphere.mesos.matcher.ResourceMatcher

case class NodeState(
  id: UUID,
  cluster: String,
  role: NodeRole.EnumVal,
  version: Long = 0,
  persistentState: Option[CephNode] = None,
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
  lazy val taskId = NodeState.makeTaskId(role, cluster, id)
  taskStatus.foreach { s =>
    require(s.getTaskId.getValue == taskId, "Critical error - TaskStatus must match generated node state")
  }

  lazy val pState = persistentState.getOrElse(
    CephNode(
      id = id,
      cluster = cluster,
      role = role))

  def peers(p: Iterable[NodeState]): Iterable[NodeState] =
    p.filter { peer => (peer.role == this.role) && peer != this }

  @deprecated("move to pState", "")
  def inferPersistedState: CephNode = pState

  /** If task is running
    */
  def runningState: Option[RunState.EnumVal] = for {
    status <- taskStatus
    launched <- pState.lastLaunched
    if (status.getState == TaskState.TASK_RUNNING)
  } yield {
    launched
  }
}

object NodeState {
  def newNode(id: UUID, cluster: String, role: NodeRole.EnumVal, persistentState: Option[CephNode])(
    implicit actorContext: ActorContext, behaviorSet: BehaviorSet): NodeState = {

    val taskId = makeTaskId(role = role, cluster = cluster, id = id)
    val taskStatus = for {
      p <- persistentState
      slaveId <- p.slaveId
    } yield ProtoHelpers.newTaskStatus(taskId, slaveId)

    NodeState(
      id = id,
      cluster = cluster,
      role = role,
      persistentState = persistentState,
      behavior = behaviorSet.defaultBehaviorFactory(taskId, actorContext),
      taskStatus = taskStatus)
  }

  def forRole(role: NodeRole.EnumVal)(
    implicit actorContext: ActorContext, behaviorSet: BehaviorSet): NodeState = {
    newNode(
      id = UUID.randomUUID,
      cluster = Constants.DefaultCluster,
      role = role,
      persistentState = None)
  }


  def fromState(state: CephNode)(
    implicit actorContext: ActorContext, behaviorSet: BehaviorSet): NodeState = {
    newNode(
      id = state.id,
      cluster = state.cluster,
      role = state.role,
      persistentState = Some(state))
  }
  def makeTaskId(role: NodeRole.EnumVal, cluster: String, id: UUID): String =
    s"${role}#${cluster}#${id.toString}"

}
