package org.vivint.ceph

import akka.actor.{ ActorContext, Cancellable }
import java.util.concurrent.atomic.AtomicInteger
import mesosphere.mesos.protos.TaskStatus
import org.apache.mesos.Protos.Offer

import TaskActor.NodeState
import org.vivint.ceph.model.CephNode
import NodeFSM._
import Behavior._
import scala.annotation.tailrec
import scala.concurrent.duration._

object NodeFSM {
  sealed trait Event
  /**
    * external event caused the node state to change
    */
  case class NodeUpdated(prior: NodeState) extends Event {
    def taskStatusChanged(current: NodeState): Boolean =
      prior.taskStatus != current.taskStatus
  }
  case class MatchedOffer(offer: Offer) extends Event
  case class Timer(id: Any) extends Event

  sealed trait Action

  case class Directive(action: Option[Action] = None, transition: Option[BehaviorFactory] = None) {
    def withTransition(b: BehaviorFactory) =
      copy(transition = Some(b))
  }

  case class Persist(data: CephNode) extends Action
  case class Hold(offer: Offer) extends Action
}

trait Directives {
  /**
    * Do nothing. Modify nothing.
    */
  final def stay = Directive()

  /**
    * Update the persistent ceph storage
    */
  final def persist(data: CephNode) = Directive(Some(Persist(data)))

  /**
    * Change the behavor out after performing any actions
    */
  final def transition(behaviorFactory: BehaviorFactory) = Directive(None, Some(behaviorFactory))

  final def hold(offer: Offer) = Directive(Some(Hold(offer)), None)
}

object Directives extends Directives

trait Behavior extends Directives {
  private var initialized = false
  val actorContext: ActorContext
  private val timers = scala.collection.mutable.Map.empty[Int, Cancellable]
  case class BehaviorTimer(timerId: Int, id: Any)

  def setBehaviorTimer(id: Any, duration: FiniteDuration): Unit = {
    import actorContext.dispatcher
    val timerId = Behavior.timerId.incrementAndGet()
    timers(timerId) = actorContext.system.scheduler.scheduleOnce(duration) {
      actorContext.self ! TaskActor.NodeTimer(taskId, BehaviorTimer(timerId, id))
    }
  }

  @tailrec final def preHandleEvent(event: Event, state: NodeState, fullState: Map[String, NodeState]): Directive = {
    event match {
      case Timer(BehaviorTimer(timerId, id)) =>
        if (timers.remove(timerId).isEmpty)
          stay
        else {
          timers.remove(timerId)
          // unwrap managed timer payload
          preHandleEvent(Timer(id), state, fullState)
        }
      case _ =>
        handleEvent(event, state, fullState)
    }
  }

  final def initialize(state: NodeState, fullState: Map[String, NodeState]): Directive =
    preStart(state, fullState)
  final def submit(event: Event, state: NodeState, fullState: Map[String, NodeState]): Directive =
    preHandleEvent(event, state, fullState)
  final def teardown(): Unit = {
    timers.values.foreach { _.cancel }
    timers.clear()
  }

  def taskId: String
  /**
    * Method provides an opportunity to set the next step
    */
  protected def preStart(state: NodeState, fullState: Map[String, NodeState]): Directive = stay
  protected def handleEvent(event: Event, state: NodeState, fullState: Map[String, NodeState]): Directive
}

object Behavior {
  val timerId = new AtomicInteger
  type BehaviorFactory = (String, ActorContext) => Behavior
  type DecideFunction = (NodeState, Map[String, NodeState]) => Directive
}

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

case class InitializeLogic(taskId: String) extends Behavior {
  override def preStart(state: NodeState, fullState: Map[String, NodeState]): Directive = {
    SharedLogic.decideWhatsNext(state: NodeState, fullState: Map[String, NodeState]): Directive
  }

  def handleEvent(event: Event, state: NodeState, fullState: Map[String, NodeState]): Directive =
    throw new IllegalStateException("handleEvent called on InitializeLogic")
}

case class WaitForSync(nextBehavior: DecideFunction)(taskId: String, actorContext: ActorContext) extends Behavior {
  override def preStart(state: NodeState, fullState: Map[String, NodeState]): Directive = {
    setBehaviorTimer("timeout", 30.seconds)
    stay
  }

  def handleEvent(event: Event, state: NodeState, fullState: Map[String, NodeState]): Directive = {
    event match {
      case Timer("timeout") =>
        nextBehavior(state, fullState)
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
