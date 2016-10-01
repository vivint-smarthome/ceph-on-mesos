package org.vivint.ceph

import akka.actor.{ ActorContext, Cancellable }
import java.util.concurrent.atomic.AtomicInteger
import mesosphere.mesos.protos.TaskStatus
import org.apache.mesos.Protos.Offer
import org.vivint.ceph.model.{ CephNode, CephConfig, NodeState }
import NodeFSM._
import Behavior._
import scala.annotation.tailrec
import scala.concurrent.duration._
import model.ClusterSecrets

class NodeBehavior(secrets: ClusterSecrets, deploymentConfig: () => CephConfig) extends BehaviorSet {
  object SharedLogic extends Directives {
    def decideWhatsNext(state: NodeState, fullState: Map[String, NodeState]): Directive = {
      state.persistentState match {
        case None =>
          persist(
            CephNode(
              id = state.id,
              cluster = state.cluster,
              role = state.role)).
            withTransition(
              WaitForSync(decideWhatsNext))
        case Some(pState) if state.version != state.persistentVersion =>
          // try again; previous persistence must have timed out.
          persist(pState).
            withTransition(
              WaitForSync(decideWhatsNext))
        case Some(state) if ! state.resourcesReserved =>
          transition(Matching)
        case Some(state) if state.resourcesReserved =>
          transition(Running)
      }
    }
  }

  case class InitializeLogic(taskId: String, actorContext: ActorContext) extends Behavior {
    override def preStart(state: NodeState, fullState: Map[String, NodeState]): Directive = {
      SharedLogic.decideWhatsNext(state: NodeState, fullState: Map[String, NodeState]): Directive
    }

    def handleEvent(event: Event, state: NodeState, fullState: Map[String, NodeState]): Directive =
      throw new IllegalStateException("handleEvent called on InitializeLogic")
  }

  case class WaitForSync(nextBehavior: DecideFunction)(val taskId: String, val actorContext: ActorContext) extends Behavior {
    override def preStart(state: NodeState, fullState: Map[String, NodeState]): Directive = {
      setBehaviorTimer("timeout", 30.seconds)
      stay
    }

    def handleEvent(event: Event, state: NodeState, fullState: Map[String, NodeState]): Directive = {
      event match {
        case Timer("timeout") =>
          nextBehavior(state, fullState)
        case Timer(_) =>
          stay
        case NodeUpdated(prior) =>
          if (state.persistentVersion < state.version)
            stay
          else
            nextBehavior(state, fullState)
        case MatchedOffer(offer) =>
          hold(offer)
      }
    }
  }

  case class Running(taskId: String, actorContext: ActorContext) extends Behavior {
    def handleEvent(event: Event, state: NodeState, fullState: Map[String, NodeState]): Directive ={
      // we're going to need to have an event loop
      ???
    }
  }

  case class Matching(taskId: String, actorContext: ActorContext) extends Behavior {
    def handleEvent(event: Event, state: NodeState, fullState: Map[String, NodeState]): Directive = {
      // TODO
      ???
    }
  }

  def defaultBehaviorFactory = InitializeLogic
}
