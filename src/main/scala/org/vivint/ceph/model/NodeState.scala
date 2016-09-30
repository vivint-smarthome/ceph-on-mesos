package org.vivint.ceph
package model

import akka.actor.ActorContext
import java.util.UUID
import org.apache.mesos.Protos._
import mesosphere.mesos.matcher.ResourceMatcher

case class NodeState(
  id: UUID,
  cluster: String,
  role: String,
  version: Long = 0,
  persistentState: Option[CephNode] = None,
  behavior: Behavior,
  persistentVersion: Long = 0,
  wantingNewOffer: Boolean = false,
  heldOffer: Option[Offer] = None,
  offerMatchers: List[ResourceMatcher] = Nil,
  taskStatus: Option[TaskStatus] = None
) {
  lazy val taskId = NodeState.makeTaskId(role, cluster, id)
  taskStatus.foreach { s =>
    require(s.getTaskId.getValue == taskId, "Critical error - TaskStatus must match generated node state")
  }
}


object NodeState {
  def newNode(id: UUID, cluster: String, role: String, persistentState: Option[CephNode])(
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

  def forRole(role: String)(
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
  def makeTaskId(role: String, cluster: String, id: UUID): String =
    s"${role}#${cluster}#${id.toString}"

}
