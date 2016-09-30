package org.vivint.ceph
import org.apache.mesos.Protos._
import scala.collection.immutable.NumericRange
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

  def newRanges(ranges: Iterable[NumericRange.Inclusive[Long]]): Value.Ranges = {
    val b = Value.Ranges.newBuilder
    ranges.foreach { r =>
      b.addRange(
        Value.Range.newBuilder.
          setBegin(r.min).
          setEnd(r.max))
    }
    b.build
  }

  def newTaskStatus(taskId: String, slaveId: String): TaskStatus =
    TaskStatus.newBuilder.
      setTaskId(TaskID.newBuilder.setValue(taskId)).
      setSlaveId(SlaveID.newBuilder.setValue(slaveId)).
      build

  implicit class RichOffer(offer: Offer) {
    def resources =
      offer.getResourcesList.toList

    def slaveId: Option[String] =
      if (offer.hasSlaveId)
        Some(offer.getSlaveId.getValue)
      else
        None

    def hostname: Option[String] =
      if (offer.hasHostname)
        Some(offer.getHostname)
      else
        None
  }

  implicit class RichResource(resource: Resource) {
    def reservation =
      if (resource.hasReservation) Some(resource.getReservation) else None

    def ranges: List[NumericRange.Inclusive[Long]] =
      if (resource.hasRanges) {
        resource.getRanges.getRangeList.toList.map { r =>
          NumericRange.inclusive(r.getBegin, r.getEnd, 1)
        }
      } else
        Nil
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
