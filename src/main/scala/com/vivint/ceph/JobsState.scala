package com.vivint.ceph

import akka.event.LoggingAdapter
import model.Job
import JobsState._
import java.util.UUID

object JobsState {
  type Subscriber = PartialFunction[(Option[Job], Option[Job]), Unit]
}

class JobsState(log: LoggingAdapter) {
  private [this] var _jobs: Map[UUID, Job] = Map.empty
  private [this] var subscribers = List.empty[Subscriber]
  def all = _jobs
  def values = _jobs.values
  def addSubscriber(subscriber: Subscriber): Unit = {
    subscribers = subscriber :: subscribers
  }

  def get(id: UUID): Option[Job] = _jobs.get(id)

  def apply(id: UUID): Job = _jobs(id)

  def contains(id: UUID): Boolean = _jobs contains id

  def getByTaskId(taskId: String) = _jobs.values.find(_.taskId.contains(taskId))

  def containsTaskId(taskId: String) = _jobs.values.exists(_.taskId.contains(taskId))

  def getByReservationId(reservationId: UUID): Option[Job] =
    _jobs.values.find(_.reservationId.contains(reservationId))

  def containsReservationId(reservationId: UUID) =
    _jobs.values.exists(_.reservationId.contains(reservationId))

  /** Given an updated task status, increments persistent state version if it has been changed. Calls all registered
    * subscribers.
    */
  def updateJob(update: Job): Job = {
    val prior = _jobs.get(update.id)
    if (prior.contains(update))
      return update

    if (update.purged) {
      log.debug("task purged: {}", model.PlayJsonFormats.JobWriter.writes(update))
      _jobs = _jobs - update.id
      callSubscribers((prior, None))
      update
    } else {
      val nextTask =
        if (_jobs.get(update.id).map(_.pState) != Some(update.pState)) {
          val nextVersion = update.version + 1
          update.copy(
            version = nextVersion)
        } else {
          update
        }
      if (log.isDebugEnabled)
        log.debug("task updated: {}", model.PlayJsonFormats.JobWriter.writes(nextTask))

      _jobs = _jobs.updated(update.id, nextTask)
      callSubscribers((prior, Some(nextTask)))

      nextTask
    }
  }


  private def callSubscribers(event: (Option[Job], Option[Job])): Unit =
    subscribers.foreach { subscriber =>
      if(subscriber.isDefinedAt(event))
        subscriber(event)
    }

  def updatePersistence(id: UUID, version: Long) = {
    _jobs.get(id) foreach { task =>
      updateJob(
        task.copy(persistentVersion = Math.max(task.persistentVersion, version)))
    }
  }
}
