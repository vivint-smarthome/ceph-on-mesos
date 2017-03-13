package com.vivint.ceph.orchestrator

import com.vivint.ceph.JobsState
import com.vivint.ceph.model.Job
import java.util.UUID
import OrchestratorFSM._

class OrchestratorFSM(initialBehavior: Behavior, jobs: JobsState) {
  var currentBehavior: Behavior = initializeBehavior(initialBehavior)

  def initializeBehavior(behavior: Behavior): Behavior =
    processDirective(behavior, behavior.preStart(jobs.all))

  def applyAction(action: Action): Unit = action match {
    case UpdateJob(job) =>
      jobs.updateJob(job)
  }

  def processDirective(behavior: Behavior, directive: Directive): Behavior = {
    directive.action.foreach(applyAction)
    directive.transition match {
      case None => behavior
      case Some(newBehavior) =>
        initializeBehavior(newBehavior)
    }
  }

  val update: JobsState.Subscriber = { case (oldJobState, newJobState) =>
    currentBehavior = processDirective(
      currentBehavior,
      currentBehavior.onUpdate(oldJobState, newJobState, jobs.all))
  }
}

object OrchestratorFSM {
  trait Behavior {
    def preStart(fullState: Map[UUID, Job]): Directive = Stay
    def onUpdate(oldJob: Option[Job], newJob: Option[Job], fullState: Map[UUID, Job]): Directive
  }

  case class Directive(action: List[Action] = Nil, transition: Option[Behavior] = None) {
    def andAlso(other: Action): Directive =
      copy(action = action :+ other)
  }
  object Directive {
    import scala.language.implicitConversions
    implicit def fromAction(action: Action): Directive = {
      Directive(List(action), None)
    }
    implicit def fromActionList(actionList: ActionList): Directive = {
      Directive(actionList.actions, None)
    }
  }

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

  final def Transition(behavior: Behavior) = Directive(Nil, Some(behavior))
  case class UpdateJob(job: Job) extends Action

  final val Stay = Directive()
}
