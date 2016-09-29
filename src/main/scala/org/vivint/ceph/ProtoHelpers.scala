package org.vivint.ceph
import org.apache.mesos.Protos._
import scala.concurrent.duration._
import scala.collection.JavaConversions._
import scala.collection.breakOut

object ProtoHelpers {
  def newFilters(refuseDuration: Option[FiniteDuration]): Filters = {
    val b = Filters.newBuilder
    refuseDuration.foreach { d =>
      b.setRefuseSeconds(d.toMillis / 1000.0)
    }
    b.build
  }

  def newRanges(ranges: List[(Long, Long)]): Value.Ranges = {
    val b = Value.Ranges.newBuilder
    ranges.foreach { case (start, end) =>
      b.addRange(
        Value.Range.newBuilder.
          setBegin(start).
          setEnd(end))
    }
    b.build
  }

  def newTaskStatus(taskId: String, slaveId: String): TaskStatus =
    TaskStatus.newBuilder.
      setTaskId(TaskID.newBuilder.setValue(taskId)).
      setSlaveId(SlaveID.newBuilder.setValue(slaveId)).
      build

  implicit class RichResource(resource: Resource) {
    def reservation =
      if (resource.hasReservation) Some(resource.getReservation) else None
  }

  implicit class RichLabels(labels: Labels) {
    def get(key: String): Option[String] =
      labels.getLabelsList.iterator.collectFirst {
        case label if label.getKey == key => label.getValue
      }

    def toMap: Map[String, String] = {
      labels.getLabelsList.map { l =>
        l.getKey -> l.getValue
      }(breakOut)
    }
  }
}
