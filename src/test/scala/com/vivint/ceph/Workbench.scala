package com.vivint.ceph

import com.vivint.ceph.model.{ IPLocation, Job, JobRole, Location, PersistentState, RunState }
import java.util.UUID
import scala.concurrent.duration._

object Workbench {
  def newAppConfiguration() = {
    AppConfiguration(master = "hai", name = "ceph-test", principal = "ceph", secret = None, role = "ceph",
      zookeeper = "zk://test", offerTimeout = 5.seconds, publicNetwork = "10.11.12.0/24",
      clusterNetwork =  "10.11.12.0/24", storageBackend = "memory")
  }

  def newPendingRGWJob(goal: Option[RunState.EnumVal] = None) = {
    PersistentState(
      id = UUID.randomUUID(),
      cluster = "ceph",
      role = JobRole.RGW,
      lastLaunched = None,
      goal = goal,
      reservationConfirmed = false,
      slaveId = None,
      reservationId = None,
      taskId = None,
      location = Location.empty)
  }

  def newRunningRGWJob(
    taskId: String = Job.makeTaskId(JobRole.RGW, "ceph"),
    location: Location = IPLocation("10.11.12.13", 31001)) = {
    PersistentState(
      id = UUID.randomUUID(),
      cluster = "ceph",
      role = JobRole.RGW,
      lastLaunched = Some(RunState.Running),
      goal = Some(RunState.Running),
      reservationConfirmed = false,
      slaveId = Some("slave-12"),
      reservationId = None,
      taskId = Some(taskId),
      location = location)
  }
  def newRunningMonitorJob(
    taskId: String = Job.makeTaskId(JobRole.Monitor, "ceph"),
    location: Location = IPLocation("10.11.12.13", 31001)) = {
    PersistentState(
      id = UUID.randomUUID(),
      cluster = "ceph",
      role = JobRole.Monitor,
      lastLaunched = Some(RunState.Running),
      goal = Some(RunState.Running),
      reservationConfirmed = true,
      slaveId = Some("slave-12"),
      reservationId = Some(UUID.randomUUID()),
      taskId = Some(taskId),
      location = location)
  }

}
