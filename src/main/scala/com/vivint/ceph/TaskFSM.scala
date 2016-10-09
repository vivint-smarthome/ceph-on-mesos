package com.vivint.ceph

import akka.actor.Cancellable
import akka.event.LoggingAdapter
import com.vivint.ceph.model.{ RunState, ServiceLocation, TaskRole }
import org.apache.mesos.Protos
import mesosphere.mesos.matcher.ResourceMatcher
import com.vivint.ceph.model.{PersistentState,Task}
import TaskFSM._
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

  private def processAction(task: Task, action: Directives.Action): Task = {
    log.debug("{} - processing directive response action {}", task.taskId, action.getClass.getName)
    action match {
      case Directives.Hold(offer, resourceMatch) =>
        // Decline existing held offer
        task.heldOffer.foreach {
          case (pending, _) => pending.resultingOperationsPromise.trySuccess(Nil)
        }
        task.copy(heldOffer = Some((offer, resourceMatch)))
      case Directives.Persist(data) =>
        task.copy(persistentState = Some(data))
      case Directives.SetBehaviorTimer(name, duration: FiniteDuration) =>
        setBehaviorTimer(task, name, duration)
        task
      case Directives.WantOffers =>
        revive()
        task.copy(wantingNewOffer = true)
      case Directives.KillTask =>
        killTask(task.taskId)
        task
      case Directives.OfferResponse(pendingOffer, operations) =>
        pendingOffer.resultingOperationsPromise.success(operations.toList)
        task
    }
  }

  final def handleEvent(task: Task, event: TaskFSM.Event): Unit = {
    tasks.updateTask(
      processEvents(task, List(event)))
  }

  private final def processDirective(task: Task, directive: Directives.Directive): Task = {
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

object Directives {
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

  case class Persist(data: PersistentState) extends Action
  case object KillTask extends Action
  case class Hold(offer: PendingOffer, resourceMatch: Option[ResourceMatcher.ResourceMatch]) extends Action
  case object WantOffers extends Action
  case class OfferResponse(offer: PendingOffer, operations: Iterable[Protos.Offer.Operation]) extends Action
  case class SetBehaviorTimer(id: String, duration: FiniteDuration) extends Action
  case class Directive(action: List[Action] = Nil, transition: Option[Behavior] = None)
  val Stay = Directive()
  final def Transition(behavior: Behavior) = Directive(Nil, Some(behavior))

  object Directive {
    import scala.language.implicitConversions
    implicit def fromAction(action: Action): Directive = {
      Directive(List(action), None)
    }
    implicit def fromActionList(actionList: ActionList): Directive = {
      Directive(actionList.actions, None)
    }
  }

}

trait Behavior {
  import Directives._
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
  def preStart(state: Task, fullState: Map[String, Task]): Directive = Stay
  def handleEvent(event: Event, state: Task, fullState: Map[String, Task]): Directive
}

object Behavior {
  type DecideFunction = (Task, Map[String, Task]) => Directives.Directive
  type TransitionFunction = (Task, Map[String, Task]) => Behavior
}

trait BehaviorSet {
  def defaultBehavior(role: model.TaskRole.EnumVal): Behavior
}
