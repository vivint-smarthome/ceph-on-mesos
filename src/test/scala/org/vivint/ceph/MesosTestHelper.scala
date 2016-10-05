package org.vivint.ceph

import mesosphere.marathon.state.ResourceRole
import org.apache.mesos.Protos.Offer
import org.apache.mesos.{Protos => Mesos}
import mesosphere.mesos.protos.Resource.{CPUS, MEM, PORTS, DISK}
import scala.collection.JavaConverters._
import scala.collection.JavaConversions._
import scala.collection.immutable.NumericRange

/** Adapted from MarathonTestHelper; TODO licensing
  */
object MesosTestHelper {
  import ProtoHelpers._
  val idx = new java.util.concurrent.atomic.AtomicInteger
  val frameworkID: Mesos.FrameworkID = newFrameworkId("ceph-framework-id")

  def makeBasicOffer(cpus: Double = 4.0, mem: Double = 16000, disk: Double = 1024.0,
    ports: NumericRange.Inclusive[Long] = (31000L to 32000L), role: String = ResourceRole.Unreserved,
    reservationLabels: Option[Mesos.Labels] = None, slaveId: Int = 0, offerId: Int = idx.incrementAndGet): Offer.Builder = {

    require(role != ResourceRole.Unreserved || reservationLabels.isEmpty, "reserved resources cannot have role *")

    def heedReserved(resource: Mesos.Resource): Mesos.Resource = {
      reservationLabels match {
        case Some(labels) =>
          val reservation =
            Mesos.Resource.ReservationInfo.newBuilder()
              .setPrincipal("ceph")
              .setLabels(labels)
          resource.toBuilder.setReservation(reservation).build()
        case None =>
          resource
      }
    }

    val cpusResource = heedReserved(newScalarResource(CPUS, cpus, role = role))
    val memResource = heedReserved(newScalarResource(MEM, mem, role = role))
    val diskResource = heedReserved(newScalarResource(DISK, disk, role = role))
    val portsResource = heedReserved(newRangesResource(PORTS, Seq(ports), role))

    val offerBuilder = Offer.newBuilder.
      setId(newOfferId(offerId.toString)).
      setFrameworkId(frameworkID).
      setSlaveId(newSlaveId(s"slave-${slaveId}")).
      setHostname("localhost").
      addResources(cpusResource).
      addResources(memResource).
      addResources(diskResource).
      addResources(portsResource)

    offerBuilder
  }

  def mountSource(path: Option[String]): Mesos.Resource.DiskInfo.Source = {
    val b = Mesos.Resource.DiskInfo.Source.newBuilder.
      setType(Mesos.Resource.DiskInfo.Source.Type.MOUNT)
    path.foreach { p =>
      b.setMount(Mesos.Resource.DiskInfo.Source.Mount.newBuilder.
        setRoot(p))
    }

    b.build
  }

  def mountSource(path: String): Mesos.Resource.DiskInfo.Source =
    mountSource(Some(path))

  def mountDisk(path: Option[String]): Mesos.Resource.DiskInfo = {
    Mesos.Resource.DiskInfo.newBuilder.
      setSource(
        mountSource(path)).
        build
  }

  def mountDisk(path: String): Mesos.Resource.DiskInfo =
    mountDisk(Some(path))

  def pathSource(path: Option[String]): Mesos.Resource.DiskInfo.Source = {
    val b = Mesos.Resource.DiskInfo.Source.newBuilder.
      setType(Mesos.Resource.DiskInfo.Source.Type.PATH)
    path.foreach { p =>
      b.setPath(Mesos.Resource.DiskInfo.Source.Path.newBuilder.
        setRoot(p))
    }

    b.build
  }

  def pathSource(path: String): Mesos.Resource.DiskInfo.Source =
    pathSource(Some(path))

  def pathDisk(path: Option[String]): Mesos.Resource.DiskInfo = {
    Mesos.Resource.DiskInfo.newBuilder.
      setSource(
        pathSource(path)).
        build
  }

  def pathDisk(path: String): Mesos.Resource.DiskInfo =
    pathDisk(Some(path))

  /**
    * crude method which currently assumes that all disk resources are represented by volumes */
  def mergeReservation(offer: Mesos.Offer,
    resources: Mesos.Offer.Operation.Reserve,
    volumes: Mesos.Offer.Operation.Create): Mesos.Offer = {

    offer.toBuilder.clearResources().
      addAllResources(resources.getResourcesList.filterNot(_.getName == DISK)).
      addAllResources(volumes.getVolumesList).
      setId(newOfferId(idx.incrementAndGet.toString)).
      build
  }
}
