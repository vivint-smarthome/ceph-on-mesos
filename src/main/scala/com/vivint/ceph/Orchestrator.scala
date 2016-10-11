package com.vivint.ceph

import com.vivint.ceph.model.{ RunState, Job, JobRole }
import java.util.UUID

class Orchestrator(jobs: JobsState) {
  class MutableDelegator(private var delegator: JobsState.Subscriber) extends JobsState.Subscriber {
    def isDefinedAt(x: (Option[Job], Option[Job])): Boolean = delegator.isDefinedAt(x)
    def apply(v1: (Option[Job], Option[Job])) = delegator.apply(v1)
    def update(d: JobsState.Subscriber): Unit =
      delegator = d
  }

  def getMonitors =
    jobs.values.filter(_.role == JobRole.Monitor)

  val currentBehavior = new MutableDelegator(start())
  jobs.addSubscriber(currentBehavior)

  def setToRunning(job: Job): Unit =
    jobs.updateJob(
      job.withGoal(Some(RunState.Running)))

  def start(): JobsState.Subscriber = {
    getMonitors.filter(_.goal.nonEmpty).toList match {
      case Nil =>
        {
          case (_, Some(job))
              if job.pState.reservationConfirmed && job.role == JobRole.Monitor =>
            jobs.updateJob(
              job.withGoal(Some(RunState.Running)))
            currentBehavior.update(waitingMonLeader(job.id))
        }
      case List(single) =>
        waitingMonLeader(single.id)
      case more =>
        waitingMonQuorum
    }
  }

  def waitingMonLeader(id: UUID): JobsState.Subscriber = {
    if (jobs(id).runningState.nonEmpty) {
      waitingMonQuorum()
    } else {
      // TODO - wait for health checks when those are implemented
      case (_, Some(job)) if job.id == id && job.runningState.nonEmpty =>
        currentBehavior.update(waitingMonQuorum())
    }
  }

  /*
   TODO - implement condition pattern (with optimization hint to know when to re-evaluate the condition)
   */
  def waitingMonQuorum(): JobsState.Subscriber = {
    getMonitors.filter(_.goal.isEmpty).foreach(setToRunning)

    def quorumMonitorsAreRunning() = {
      val monitors = getMonitors.toList
      val runningMonitors = monitors.filter(_.runningState == Some(RunState.Running)).toList
      runningMonitors.length > (monitors.length / 2) // NOTE this always fails for mon count [0, 1]
    }

    if (quorumMonitorsAreRunning())
      launchAll()
    else {
      case (_, Some(job)) if job.role == JobRole.Monitor =>
        if (quorumMonitorsAreRunning())
          currentBehavior.update(launchAll())
    }
  }

  def launchAll(): JobsState.Subscriber = {
    jobs.values.filter(_.goal.isEmpty).foreach(setToRunning)

    {
      case (_, Some(job)) if job.goal.isEmpty =>
        setToRunning(job)
    }
  }
}
