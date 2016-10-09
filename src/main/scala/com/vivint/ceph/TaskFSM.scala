package com.vivint.ceph

import akka.actor.{ ActorContext, Cancellable }
import akka.event.LoggingAdapter
import com.vivint.ceph.model.TaskRole
import java.util.concurrent.atomic.AtomicInteger
import mesosphere.mesos.matcher.ResourceMatcher
import mesosphere.mesos.protos.TaskStatus
import org.apache.mesos.Protos.{FrameworkID, Offer}
import com.vivint.ceph.model.{PersistentState,Task}
import TaskFSM._
import Behavior._
import scala.annotation.tailrec
import scala.concurrent.duration._
import scala.collection.immutable.Iterable

object TaskFSM {
  sealed trait Event
  /**
    * external event caused the task state to change
    */
  case class TaskUpdated(prior: Task) extends Event {
    def taskStatusChanged(current: Task): Boolean =
      prior.taskStatus != current.taskStatus
  }
  case class MatchedOffer(offer: PendingOffer, matchResult: Option[ResourceMatcher.ResourceMatch]) extends Event
  case class Timer(id: Any) extends Event

  sealed trait Action {
    def withTransition(b: Behavior): Directive =
      Directive(List(this), Some(b))
    def andAlso(other: Action): ActionList =
      ActionList(this :: other :: Nil)
  }

  case class ActionList(actions: List[Action]) {
    def withTransition(b: Behavior): Directive =
      Directive(actions, Some(b))

    def andAlso(other: Action): ActionList =
      ActionList(actions :+ other)
  }

  case class Directive(action: List[Action] = Nil, transition: Option[Behavior] = None)

  object Directive {
    import scala.language.implicitConversions
    implicit def fromAction(action: Action): Directive = {
      Directive(List(action), None)
    }
    implicit def fromActionList(actionList: ActionList): Directive = {
      Directive(actionList.actions, None)
    }
  }

  case class Persist(data: PersistentState) extends Action
  case object KillTask extends Action
  case class Hold(offer: PendingOffer, resourceMatch: Option[ResourceMatcher.ResourceMatch]) extends Action
  case object WantOffers extends Action
  case class OfferResponse(offer: PendingOffer, operations: Iterable[Offer.Operation]) extends Action
  case class SetBehaviorTimer(id: String, duration: FiniteDuration) extends Action
}

class TaskFSM(tasks: TasksState, log: LoggingAdapter, behaviorSet: BehaviorSet,
  setTimer: (String, String, FiniteDuration) => Cancellable,
  revive: () => Unit,
  killTask: (String => Unit)
) {
  // TODO - for each task
  type TaskId = String
  type TimerName = String
  import scala.collection.mutable
  private val taskTimers = mutable.Map.empty[TaskId, mutable.Map[TimerName, Cancellable]].
    withDefaultValue(mutable.Map.empty)

  tasks.addSubscriber {
    case (Some(before), Some(after)) =>
      handleEvent(after, TaskFSM.TaskUpdated(before))
  }

  private def setBehaviorTimer(task: Task, timerName: TimerName, duration: FiniteDuration): Unit = {
    val timers = taskTimers(task.taskId)
    timers(timerName) = setTimer(task.taskId, timerName, duration)
  }

  private def clearTimers(task: Task): Unit =
    taskTimers.
      remove(task.taskId).
      getOrElse(mutable.Map.empty).
      foreach { case (_, cancellable) =>
        cancellable.cancel()
      }

  def onTimer(taskId: TaskId, timerName: TimerName): Unit = {
    for {
      cancellable <- taskTimers(taskId).remove(timerName)
      task <- tasks.get(taskId)
    } {
      cancellable.cancel() // just in case it was manually invoked?
      log.debug("Timer {} for taskId {} fired", timerName, taskId)
      handleEvent(task, TaskFSM.Timer(timerName))
    }
  }

  private def processEvents(task: Task, events: List[TaskFSM.Event]): Task = events match {
    case event :: rest =>
      log.debug("{} - sending event {}", task.taskId, event.getClass.getName)

      processEvents(
        processDirective(
          task,
          task.behavior.submit(event, task, tasks.all)),
        rest)
    case Nil =>
      task
  }

  private def processHeldEvents(task: Task): Task = {
    task.heldOffer match {
      case Some((offer, resourceMatch)) =>
        processEvents(
          task.copy(heldOffer = None),
          TaskFSM.MatchedOffer(offer, resourceMatch) :: Nil)
      case None =>
        task
    }
  }

  private def processAction(task: Task, action: TaskFSM.Action): Task = {
    log.debug("{} - processing directive response action {}", task.taskId, action.getClass.getName)
    action match {
      case TaskFSM.Hold(offer, resourceMatch) =>
        // Decline existing held offer
        task.heldOffer.foreach {
          case (pending, _) => pending.resultingOperationsPromise.trySuccess(Nil)
        }
        task.copy(heldOffer = Some((offer, resourceMatch)))
      case TaskFSM.Persist(data) =>
        task.copy(persistentState = Some(data))
      case TaskFSM.SetBehaviorTimer(name, duration: FiniteDuration) =>
        setBehaviorTimer(task, name, duration)
        task
      case TaskFSM.WantOffers =>
        revive()
        task.copy(wantingNewOffer = true)
      case TaskFSM.KillTask =>
        killTask(task.taskId)
        task
      case TaskFSM.OfferResponse(pendingOffer, operations) =>
        pendingOffer.resultingOperationsPromise.success(operations.toList)
        task
    }
  }

  final def handleEvent(task: Task, event: TaskFSM.Event): Unit = {
    tasks.updateTask(
      processEvents(task, List(event)))
  }

  private final def processDirective(task: Task, directive: TaskFSM.Directive): Task = {
    val taskAfterAction = directive.action.foldLeft(task)(processAction)

    directive.transition match {
      case Some(nextBehavior) =>
        clearTimers(task)
        log.info("task {}: Transition {} -> {}", task.taskId, task.behavior.name, nextBehavior.name)
        processHeldEvents(
          initializeBehavior(taskAfterAction.copy(behavior = nextBehavior)))

      case None =>
        taskAfterAction
    }
  }

  final def initializeBehavior(task: Task): Task = {
    log.info("task {}: Initializing behavior {}", task.taskId, task.behavior.name)
    processDirective(task,
      task.behavior.preStart(task, tasks.all))
  }

  def defaultBehavior(role: TaskRole.EnumVal): Behavior =
    behaviorSet.defaultBehavior(role)

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
  final def transition(behavior: Behavior) = Directive(Nil, Some(behavior))

  final val hold = Hold(_, _)
  final val multi = ActionList(_)
  final val wantOffers = WantOffers
  final val offerResponse = OfferResponse(_, _)
  final val killTask = KillTask
  final val setBehaviorTimer = SetBehaviorTimer
}

object Directives extends Directives

trait Behavior extends Directives {
  def name = getClass.getSimpleName

  @deprecated("use preStart", "now")
  final def initialize(state: Task, fullState: Map[String, Task]): Directive =
    preStart(state, fullState)

  @deprecated("use handleEvent", "now")
  final def submit(event: Event, state: Task, fullState: Map[String, Task]): Directive =
    handleEvent(event, state, fullState)

  /**
    * Method provides an opportunity to set the next step
    */
  def preStart(state: Task, fullState: Map[String, Task]): Directive = stay
  def handleEvent(event: Event, state: Task, fullState: Map[String, Task]): Directive
}

object Behavior {
  type DecideFunction = (Task, Map[String, Task]) => Directive
  type TransitionFunction = (Task, Map[String, Task]) => Behavior
}

trait BehaviorSet {
  def defaultBehavior(role: model.TaskRole.EnumVal): Behavior
}
