package com.vivint.ceph

import akka.actor.Cancellable
import akka.event.LoggingAdapter
import com.vivint.ceph.model.{ RunState, ServiceLocation, JobRole }
import java.util.UUID
import org.apache.mesos.Protos
import mesosphere.mesos.matcher.ResourceMatcher
import com.vivint.ceph.model.{PersistentState,Job}
import JobFSM._
import scala.concurrent.duration._
import scala.collection.immutable.Iterable

object JobFSM {
  sealed trait Event
  /**
    * external event caused the task state to change
    */
  case class JobUpdated(prior: Job) extends Event {
    def taskStatusChanged(current: Job): Boolean =
      prior.taskStatus != current.taskStatus
  }
  case class MatchedOffer(offer: PendingOffer, matchResult: Option[ResourceMatcher.ResourceMatch]) extends Event
  case class Timer(id: Any) extends Event

}

class JobFSM(jobs: JobsState, log: LoggingAdapter, behaviorSet: BehaviorSet,
  setTimer: (UUID, String, FiniteDuration) => Cancellable,
  revive: () => Unit,
  killTask: (String => Unit)
) {
  // TODO - for each task
  type JobId = UUID
  type TimerName = String
  import scala.collection.mutable
  private val jobTimers = mutable.Map.empty[JobId, mutable.Map[TimerName, Cancellable]].
    withDefaultValue(mutable.Map.empty)

  jobs.addSubscriber {
    case (Some(before), Some(after)) =>
      handleEvent(after, JobFSM.JobUpdated(before))
  }

  private def setBehaviorTimer(job: Job, timerName: TimerName, duration: FiniteDuration): Unit = {
    val timers = jobTimers(job.id)
    timers.get(timerName).foreach(_.cancel())
    timers(timerName) = setTimer(job.id, timerName, duration)
  }

  private def clearTimers(job: Job): Unit =
    jobTimers.
      remove(job.id).
      getOrElse(mutable.Map.empty).
      foreach { case (_, cancellable) =>
        cancellable.cancel()
      }

  def onTimer(jobId: JobId, timerName: TimerName): Unit = {
    for {
      cancellable <- jobTimers(jobId).remove(timerName)
      job <- jobs.get(jobId)
    } {
      cancellable.cancel() // just in case it was manually invoked?
      log.debug("Timer {} for jobId {} fired", timerName, jobId)
      handleEvent(job, JobFSM.Timer(timerName))
    }
  }

  private def processEvents(job: Job, events: List[JobFSM.Event]): Job = events match {
    case event :: rest =>
      log.debug("{} - sending event {}", job.id, event.getClass.getName)

      processEvents(
        processDirective(
          job,
          job.behavior.handleEvent(event, job, jobs.all)),
        rest)
    case Nil =>
      job
  }

  private def processHeldEvents(job: Job): Job = {
    job.heldOffer match {
      case Some((offer, resourceMatch)) =>
        processEvents(
          job.copy(heldOffer = None),
          JobFSM.MatchedOffer(offer, resourceMatch) :: Nil)
      case None =>
        job
    }
  }

  private def processAction(job: Job, action: Directives.Action): Job = {
    log.debug("{} - processing directive response action {}", job.id, action.getClass.getName)
    action match {
      case Directives.Hold(offer, resourceMatch) =>
        // Decline existing held offer
        job.heldOffer.foreach {
          case (pending, _) => pending.resultingOperationsPromise.trySuccess(Nil)
        }
        job.copy(heldOffer = Some((offer, resourceMatch)))
      case Directives.Persist(data) =>
          job.copy(
            pState = data,
            taskState = if (job.taskId != data.taskId) None else job.taskState
          )
      case Directives.SetBehaviorTimer(name, duration: FiniteDuration) =>
        setBehaviorTimer(job, name, duration)
        job
      case Directives.Revive =>
        revive()
        job
      case Directives.WantOffers =>
        if (job.heldOffer.isEmpty) {
          revive()
          job.copy(wantingNewOffer = true)
        } else {
          job
        }
      case Directives.KillTask =>
        job.taskId.foreach(killTask)
        job
      case Directives.OfferResponse(pendingOffer, operations) =>
        pendingOffer.resultingOperationsPromise.trySuccess(operations.toList)
        job
    }
  }

  final def handleEvent(job: Job, event: JobFSM.Event): Unit = {
    jobs.updateJob(
      processEvents(job, List(event)))
  }

  final def initialize(job: Job): Unit = {
    jobs.updateJob(
      initializeBehavior(job))
  }

  private final def processDirective(job: Job, directive: Directives.Directive): Job = {
    val jobAfterAction = directive.action.foldLeft(job)(processAction)

    directive.transition match {
      case Some(nextBehavior) =>
        clearTimers(job)
        log.info("job {}: Transition {} -> {}", job.id, job.behavior.name, nextBehavior.name)
        processHeldEvents(
          initializeBehavior(jobAfterAction.copy(behavior = nextBehavior)))

      case None =>
        jobAfterAction
    }
  }

  private final def initializeBehavior(job: Job): Job = {
    log.info("job {}: Initializing behavior {}", job.id, job.behavior.name)
    val maybeRemoveHeldOffer =
      if (job.heldOffer.map(_._1.resultingOperationsPromise.isCompleted).contains(true))
        job.copy(heldOffer = None)
      else
        job
    processDirective(maybeRemoveHeldOffer,
      job.behavior.preStart(maybeRemoveHeldOffer, jobs.all))
  }

  def defaultBehavior(role: JobRole.EnumVal): Behavior =
    behaviorSet.defaultBehavior(role)
}

object Directives {
  sealed trait Action {
    def withTransition(b: Behavior): Directive =
      Directive(List(this), Some(b))
    def andAlso(other: Action): ActionList =
      ActionList(this :: other :: Nil)
    def andAlso(other: Directive): Directive =
      other.copy(action = this :: other.action)
  }

  case class ActionList(actions: List[Action]) {
    def withTransition(b: Behavior): Directive =
      Directive(actions, Some(b))

    def andAlso(other: Action): ActionList =
      ActionList(actions :+ other)
  }

  /** Update the persistent state for a job. State is stored asynchronously and success can be tracked by version and
    * persistentVersion job fields.
    * If changing taskId then TaskState is assumed unknown (None)
    */
  case class Persist(data: PersistentState) extends Action
  case object KillTask extends Action
  case class Hold(offer: PendingOffer, resourceMatch: Option[ResourceMatcher.ResourceMatch]) extends Action
  case object WantOffers extends Action
  case object Revive extends Action
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
  lazy val name = getClass.getSimpleName.replace("$", "")

  @deprecated("use preStart", "now")
  final def initialize(state: Job, fullState: Map[UUID, Job]): Directive =
    preStart(state, fullState)

  @deprecated("use handleEvent", "now")
  final def submit(event: Event, state: Job, fullState: Map[UUID, Job]): Directive =
    handleEvent(event, state, fullState)

  /**
    * Method provides an opportunity to set the next step
    */
  def preStart(state: Job, fullState: Map[UUID, Job]): Directive = Stay
  def handleEvent(event: Event, state: Job, fullState: Map[UUID, Job]): Directive

  protected def handleWith(event: Event)(handler: PartialFunction[JobFSM.Event, Directive]): Directive = {
    if (handler.isDefinedAt(event))
      handler(event)
    else
      Stay
  }
}

object Behavior {
  type DecideFunction = (Job, Map[UUID, Job]) => Directives.Directive
  type TransitionFunction = (Job, Map[UUID, Job]) => Behavior
}

trait BehaviorSet {
  def defaultBehavior(role: model.JobRole.EnumVal): Behavior
}
