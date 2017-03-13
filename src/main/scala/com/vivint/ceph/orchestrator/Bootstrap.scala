package com.vivint.ceph.orchestrator

import com.vivint.ceph.model.{ RunState, Job, JobRole }
import java.util.UUID
import OrchestratorFSM._

object Bootstrap {
  def isMonitor(job: Job) =
    job.role == JobRole.Monitor

  case object Start extends Behavior {
    override def preStart(fullState: Map[UUID, Job]): Directive =
      fullState.values.filter(_.goal.nonEmpty).toList match {
        case Nil =>
          Stay
        case List(single) =>
          Transition(WaitingMonLeader(single.id))
        case more =>
          Transition(WaitingMonQuorum)
      }

    override def onUpdate(oldJob: Option[Job], newJob: Option[Job], fullState: Map[UUID, Job]): Directive =
      newJob match {
        case Some(job)
            if job.pState.reservationConfirmed && job.role == JobRole.Monitor =>

          UpdateJob(
            job.withGoal(Some(RunState.Running))).
            andAlso(
              Transition(WaitingMonLeader(job.id)))
        case _ =>
          Stay
      }
  }

  // TODO - wait for ready / health check to pass when those are implemented
  case class WaitingMonLeader(id: UUID) extends Behavior {
    override def preStart(fullState: Map[UUID, Job]): Directive =
      if (fullState(id).runningState.nonEmpty)
        Transition(WaitingMonQuorum)
      else
        Stay

    override def onUpdate(oldJob: Option[Job], newJob: Option[Job], fullState: Map[UUID, Job]): Directive = {
      newJob match {
        case Some(job) if job.id == id && job.runningState.nonEmpty =>
          Transition(WaitingMonQuorum)
        case _ =>
          Stay
      }
    }
  }

  /*
   TODO - implement condition pattern (with optimization hint to know when to re-evaluate the condition)
   */
  case object WaitingMonQuorum extends Behavior {
    private def getMonitors(fullState: Map[UUID, Job]) = {
      fullState.values.view.filter(isMonitor)
    }

    override def preStart(fullState: Map[UUID, Job]): Directive = {
      Directive(
        getMonitors(fullState).
          filter(_.goal.isEmpty).
          map(_.withGoal(Some(RunState.Running))).
          map(UpdateJob).
          toList)
    }

    override def onUpdate(oldJob: Option[Job], newJob: Option[Job], fullState: Map[UUID, Job]): Directive = {
      if (quorumMonitorsAreRunning(fullState))
        Transition(Up)
      else newJob match {
        case Some(job) if job.role == JobRole.Monitor =>
          if (quorumMonitorsAreRunning(fullState))
            Transition(Up)
          else
            Stay
      }
    }

    private def quorumMonitorsAreRunning(fullState: Map[UUID, Job]) = {
      val monitors = getMonitors(fullState)
      val runningMonitors = monitors.filter(_.runningState == Some(RunState.Running))
      runningMonitors.size > (monitors.size / 2) // NOTE this always fails for mon count [0, 1]
    }
  }

  /**
    * Behavior when we are up - we give every task a goal of running
    */
  case object Up extends Behavior {
    override def preStart(fullState: Map[UUID, Job]): Directive = {
      fullState.values.
        iterator.
        filter(_.goal.isEmpty).
        map(_.withGoal(Some(RunState.Running))).
        map(UpdateJob).
        foldLeft(Stay) { _ andAlso _ }
    }

    override def onUpdate(oldJob: Option[Job], newJob: Option[Job], fullState: Map[UUID, Job]): Directive = {
      newJob match {
        case Some(job) if job.goal.isEmpty =>
          UpdateJob(job.withGoal(Some(RunState.Running)))
        case _ =>
          Stay
      }
    }
  }
}
