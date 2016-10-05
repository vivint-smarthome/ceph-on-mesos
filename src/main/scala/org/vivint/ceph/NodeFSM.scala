package org.vivint.ceph

import akka.actor.{ ActorContext, Cancellable }
import java.util.concurrent.atomic.AtomicInteger
import mesosphere.mesos.matcher.ResourceMatcher
import mesosphere.mesos.protos.TaskStatus
import org.apache.mesos.Protos.{FrameworkID, Offer}
import org.vivint.ceph.model.{CephNode,NodeState}
import NodeFSM._
import Behavior._
import scala.annotation.tailrec
import scala.concurrent.duration._
import scala.collection.immutable.Iterable

object NodeFSM {
  sealed trait Event
  /**
    * external event caused the node state to change
    */
  case class NodeUpdated(prior: NodeState) extends Event {
    def taskStatusChanged(current: NodeState): Boolean =
      prior.taskStatus != current.taskStatus
  }
  case class MatchedOffer(offer: PendingOffer, matchResult: Option[ResourceMatcher.ResourceMatch]) extends Event
  case class Timer(id: Any) extends Event

  sealed trait Action {
    def withTransition(b: BehaviorFactory): Directive =
      Directive(List(this), Some(b))
    def andAlso(other: Action): ActionList =
      ActionList(this :: other :: Nil)
  }

  case class ActionList(actions: List[Action]) {
    def withTransition(b: BehaviorFactory): Directive =
      Directive(actions, Some(b))

    def andAlso(other: Action): ActionList =
      ActionList(actions :+ other)
  }

  case class Directive(action: List[Action] = Nil, transition: Option[BehaviorFactory] = None)

  object Directive {
    import scala.language.implicitConversions
    implicit def fromAction(action: Action): Directive = {
      Directive(List(action), None)
    }
    implicit def fromActionList(actionList: ActionList): Directive = {
      Directive(actionList.actions, None)
    }
  }

  case class Persist(data: CephNode) extends Action
  case class Hold(offer: PendingOffer, resourceMatch: Option[ResourceMatcher.ResourceMatch]) extends Action
  case object WantOffers extends Action
  case class OfferResponse(offer: PendingOffer, operations: Iterable[Offer.Operation]) extends Action
}

trait Directives {
  /**
    * Do nothing. Modify nothing.
    */
  final val stay = Directive()

  /**
    * Update the persistent ceph storage
    */
  final val persist = Persist(_)

  /**
    * Change the behavor out after performing any actions
    */
  final def transition(behaviorFactory: BehaviorFactory) = Directive(Nil, Some(behaviorFactory))

  final val hold = Hold(_, _)
  final val multi = ActionList(_)
  final val wantOffers = WantOffers
  final val offerResponse = OfferResponse(_, _)
}

object Directives extends Directives

trait Behavior extends Directives {
  private var initialized = false
  def name = getClass.getSimpleName
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

trait BehaviorSet {
  def defaultBehaviorFactory: BehaviorFactory
}
