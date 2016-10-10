package com.vivint.ceph

import akka.event.LoggingAdapter
import model.Task
import TasksState._
object TasksState {
  type Subscriber = PartialFunction[(Option[Task], Option[Task]), Unit]
}

class TasksState(log: LoggingAdapter) {
  private [this] var _tasks: Map[String, Task] = Map.empty
  private [this] var subscribers = List.empty[Subscriber]
  def all = _tasks
  def values = _tasks.values
  def addSubscriber(subscriber: Subscriber): Unit = {
    subscribers = subscriber :: subscribers
  }

  def get(taskId: String): Option[Task] = _tasks.get(taskId)

  def apply(taskId: String): Task = _tasks(taskId)

  def contains(taskId: String): Boolean = _tasks contains taskId

  /** Given an updated task status, increments persistent state version if it has been changed. Calls all registered
    * subscribers.
    */
  def updateTask(update: Task): Task = {
    val prior = _tasks.get(update.taskId)
    if (prior.contains(update))
      return update

    val nextTask =
      if (_tasks.get(update.taskId).map(_.pState) != Some(update.pState)) {
        val nextVersion = update.version + 1
        update.copy(
          version = nextVersion)
      } else {
        update
      }

    if (log.isDebugEnabled)
      log.debug("task updated: {}", model.PlayJsonFormats.TaskWriter.writes(nextTask))

    val event = (prior, Some(nextTask))
    _tasks = _tasks.updated(update.taskId, nextTask)
    subscribers.foreach { subscriber =>
      if(subscriber.isDefinedAt(event))
        subscriber(event)
    }

    nextTask
  }

  def updatePersistence(taskId: String, version: Long) = {
    _tasks.get(taskId) foreach { task =>
      updateTask(
        task.copy(persistentVersion = Math.max(task.persistentVersion, version)))
    }
  }
}
