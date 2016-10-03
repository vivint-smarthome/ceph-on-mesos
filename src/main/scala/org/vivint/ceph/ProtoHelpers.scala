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

  def newDisk(source: Option[Resource.DiskInfo.Source]): Resource.DiskInfo = {
    val b = Resource.DiskInfo.newBuilder
    source.foreach(b.setSource)
    b.build
  }

  def newOfferOperation(
    unreserve: Option[Offer.Operation.Unreserve] = None,
    destroy: Option[Offer.Operation.Destroy] = None) = {
    val b = Offer.Operation.newBuilder
    destroy.foreach(b.setDestroy)
    unreserve.foreach(b.setUnreserve)
    b.build
  }

  def newUnreserveOperation(resources: Iterable[Resource]): Offer.Operation.Unreserve = {
    val b = Offer.Operation.Unreserve.newBuilder
    resources.foreach(b.addResources)
    b.build
  }

  def newDestroyOperation(resources: Iterable[Resource]): Offer.Operation.Destroy = {
    val b = Offer.Operation.Destroy.newBuilder
    resources.foreach(b.addVolumes)
    b.build
  }

  def newLabels(kvs: (String, String)*): Labels = {
    val b = Labels.newBuilder

    kvs.foreach { case (key, value) =>
      b.addLabels(
        Label.newBuilder.setKey(key).setValue(value))
    }
    b.build
  }

  def newTaskId(taskId: String): TaskID = {
    TaskID.newBuilder.
      setValue(taskId).
      build
  }

  def newVolume(containerPath: String, hostPath: String, mode: Volume.Mode = Volume.Mode.RW) = {
    Volume.newBuilder.
      setContainerPath(containerPath).
      setHostPath(hostPath).
      setMode(mode).
      build
  }

  def newVariable(name: String, value: String) =
    Environment.Variable.newBuilder.
      setName(name).
      setValue(value).
      build

  def newEnvironment(vars: (String, String)*) = {
    val b = Environment.newBuilder
    vars.foreach { case (name, value) => b.addVariables(newVariable(name, value)) }
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

  def newScalar(amount: Double): Value.Scalar = {
    Value.Scalar.newBuilder.setValue(amount).build
  }

  def newTaskStatus(taskId: String, slaveId: String): TaskStatus =
    TaskStatus.newBuilder.
      setTaskId(TaskID.newBuilder.setValue(taskId)).
      setSlaveId(SlaveID.newBuilder.setValue(slaveId)).
      build

  implicit class RichOffer(offer: Offer) {
    def resources =
      offer.getResourcesList.toList

    def withResources(resources: Iterable[Resource]): Offer = {
      val b = offer.toBuilder.clearResources()
      resources.foreach(b.addResources)
      b.build
    }

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

  implicit class RichReservationInfo(reservation: Resource.ReservationInfo) {
    def labels =
      if (reservation.hasLabels) Some(reservation.getLabels) else None
  }

  implicit class RichResource(resource: Resource) {

    def reservation =
      if (resource.hasReservation) Some(resource.getReservation) else None

    def diskSourceOption: Option[Resource.DiskInfo.Source] =
      if (resource.hasDisk && resource.getDisk.hasSource)
        Some(resource.getDisk.getSource)
      else
        None
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
