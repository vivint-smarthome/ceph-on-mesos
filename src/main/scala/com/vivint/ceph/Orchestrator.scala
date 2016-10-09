package com.vivint.ceph

import com.vivint.ceph.model.{ RunState, Task, TaskRole }

class Orchestrator(tasks: TasksState) {
  class MutableDelegator(private var delegator: TasksState.Subscriber) extends TasksState.Subscriber {
    def isDefinedAt(x: (Option[Task], Option[Task])): Boolean = delegator.isDefinedAt(x)
    def apply(v1: (Option[Task], Option[Task])) = delegator.apply(v1)
    def update(d: TasksState.Subscriber): Unit =
      delegator = d
  }

  def getMonitors =
    tasks.values.filter(_.role == TaskRole.Monitor)

  val currentBehavior = new MutableDelegator(start())
  tasks.addSubscriber(currentBehavior)

  def setToRunning(task: Task): Unit =
    tasks.updateTask(
      task.withGoal(Some(RunState.Running)))

  def start(): TasksState.Subscriber = {
    getMonitors.filter(_.goal.nonEmpty).toList match {
      case Nil =>
        {
          case (_, Some(task))
              if task.pState.reservationConfirmed && task.role == TaskRole.Monitor =>
            tasks.updateTask(
              task.withGoal(Some(RunState.Running)))
            currentBehavior.update(waitingMonLeader(task.taskId))
        }
      case List(single) =>
        waitingMonLeader(single.taskId)
      case more =>
        waitingMonQuorum
    }
  }

  def waitingMonLeader(taskId: String): TasksState.Subscriber = {
    if (tasks(taskId).runningState.nonEmpty) {
      waitingMonQuorum()
    } else {
      // TODO - wait for health checks when those are implemented
      case (_, Some(task)) if task.taskId == taskId && task.runningState.nonEmpty =>
        currentBehavior.update(waitingMonQuorum())
    }
  }

  /*
   TODO - implement condition pattern (with optimization hint to know when to re-evaluate the condition)
   */
  def waitingMonQuorum(): TasksState.Subscriber = {
    getMonitors.filter(_.goal.isEmpty).foreach(setToRunning)

    def quorumMonitorsAreRunning() = {
      val monitors = getMonitors.toList
      val runningMonitors = monitors.filter(_.runningState == Some(RunState.Running)).toList
      runningMonitors.length > (monitors.length / 2) // NOTE this always fails for mon count [0, 1]
    }

    if (quorumMonitorsAreRunning())
      launchAll()
    else {
      case (_, Some(task)) if task.role == TaskRole.Monitor =>
        if (quorumMonitorsAreRunning())
          currentBehavior.update(launchAll())
    }
  }

  def launchAll(): TasksState.Subscriber = {
    tasks.values.filter(_.goal.isEmpty).foreach(setToRunning)

    {
      case (_, Some(task)) if task.goal.isEmpty =>
        setToRunning(task)
    }
  }
}
