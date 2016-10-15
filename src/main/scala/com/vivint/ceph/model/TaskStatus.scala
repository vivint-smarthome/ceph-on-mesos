package com.vivint.ceph
package model

import org.apache.mesos.Protos

case class TaskStatus(taskId: String, slaveId: String, state: TaskState.EnumVal) {
  def toMesos: Protos.TaskStatus = {
    ProtoHelpers.newTaskStatus(taskId, slaveId, Protos.TaskState.valueOf(state.id))
  }
}

object TaskStatus extends ((String, String, TaskState.EnumVal) => TaskStatus) {
  def fromMesos(p: Protos.TaskStatus) = {
    TaskStatus(
      p.getTaskId.getValue,
      p.getSlaveId.getValue,
      TaskState.valuesById(p.getState.getNumber))
  }
}
