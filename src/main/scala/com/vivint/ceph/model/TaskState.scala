package com.vivint.ceph
package model

import org.apache.mesos.Protos
import scala.collection.breakOut

object TaskState extends lib.Enum {
  sealed trait EnumVal extends Value {
    val id: Int
  }

  sealed trait Active extends EnumVal
  sealed trait Limbo extends EnumVal
  sealed trait Terminal extends EnumVal

  case object TaskStarting extends Active   { val id = Protos.TaskState.TASK_STARTING_VALUE ; val name = "TASK_STARTING" }
  case object TaskStaging  extends Active   { val id = Protos.TaskState.TASK_STAGING_VALUE  ; val name = "TASK_STAGING" }
  case object TaskRunning  extends Active   { val id = Protos.TaskState.TASK_RUNNING_VALUE  ; val name = "TASK_RUNNING" }
  case object TaskKilling  extends Active   { val id = Protos.TaskState.TASK_KILLING_VALUE  ; val name = "TASK_KILLING" }
  case object TaskFinished extends Terminal { val id = Protos.TaskState.TASK_FINISHED_VALUE ; val name = "TASK_FINISHED" }
  case object TaskFailed   extends Terminal { val id = Protos.TaskState.TASK_FAILED_VALUE   ; val name = "TASK_FAILED" }
  case object TaskKilled   extends Terminal { val id = Protos.TaskState.TASK_KILLED_VALUE   ; val name = "TASK_KILLED" }
  case object TaskError    extends Terminal { val id = Protos.TaskState.TASK_ERROR_VALUE    ; val name = "TASK_ERROR" }
  case object TaskLost     extends Limbo    { val id = Protos.TaskState.TASK_LOST_VALUE     ; val name = "TASK_LOST" }

  val values = Vector(TaskStarting, TaskStaging, TaskRunning, TaskKilling, TaskFinished, TaskFailed, TaskKilled,
    TaskError, TaskLost)

  val valuesById: Map[Int, EnumVal] =
    values.map { v => v.id -> v}(breakOut)

  def fromMesos(p: Protos.TaskState): TaskState.EnumVal = {
    TaskState.valuesById(p.getNumber)
  }

}
