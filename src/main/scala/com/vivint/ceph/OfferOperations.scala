package com.vivint.ceph

import akka.actor.{ Actor, ActorContext, ActorLogging, ActorRef, Cancellable, FSM, Kill, Props, Stash }
import akka.pattern.pipe
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{ Flow, Keep, Sink }
import java.util.UUID
import java.util.concurrent.TimeoutException
import lib.FutureHelpers.tSequence
import mesosphere.marathon.state.{ PersistentVolume, PersistentVolumeInfo, DiskSource }
import mesosphere.mesos.matcher._
import mesosphere.mesos.protos
import org.apache.mesos.Protos._
import scala.collection.immutable.Seq
import scala.concurrent.Future
import mesosphere.mesos.protos.Resource.DISK
import scaldi.Injector
import scaldi.Injectable._

class OfferOperations(implicit inj: Injector) {
  private val config = inject[AppConfiguration]
  import ProtoHelpers._
  /** Destroy the disks associated with this offer
    */
  def unreserveOffer(resources: Iterable[Resource]): Seq[Offer.Operation] = {
    val volumesToDestroy = resources.
      filter { r => r.hasDisk && r.getDisk.hasPersistence }

    val offersToRelease = resources.
      filter(_.hasReservation)

    Seq(
      newOfferOperation(newDestroyOperation(volumesToDestroy)),
      newOfferOperation(newUnreserveOperation(resources)))
  }

  def volId(reservationId: UUID, containerPath: String): String =
    reservationId + "#" + containerPath

  /** Adapated from Marathon code.
    * TODO - put appropriate copyright information
    */
  def createVolumes(
    frameworkId: FrameworkID,
    jobId: UUID,
    reservationId: UUID,
    localVolumes: Iterable[(DiskSource, PersistentVolume)]): Offer.Operation = {
    import scala.collection.JavaConverters._

    val volumes: Iterable[Resource] = localVolumes.map {
      case (source, vol) =>
        val disk = {
          val persistence = Resource.DiskInfo.Persistence.newBuilder().
            setId(volId(reservationId = reservationId, containerPath = vol.containerPath)).
            setPrincipal(config.principal)

          val volume = Volume.newBuilder.
            setContainerPath(vol.containerPath).
            setMode(vol.mode)

          val builder = Resource.DiskInfo.newBuilder.
            setPersistence(persistence).
            setVolume(volume)
          source.asMesos.foreach(builder.setSource)
          builder
        }

        val reservation = Resource.ReservationInfo.newBuilder.
          setLabels(newLabels(
            Constants.FrameworkIdLabel -> frameworkId.getValue,
            Constants.ReservationIdLabel -> reservationId.toString,
            Constants.JobIdLabel -> jobId.toString)).
          setPrincipal(config.principal)

        Resource.newBuilder.
          setName(DISK).
          setType(Value.Type.SCALAR).
          setScalar(newScalar(vol.persistent.size.toDouble)).
          setRole(config.role).
          setReservation(reservation).
          setDisk(disk).
          build()
    }

    val create = Offer.Operation.Create.newBuilder().
      addAllVolumes(volumes.asJava)

    newOfferOperation(create.build)
  }

  /** Adapated from Marathon code.
    * TODO - put appropriate copyright information */
  def reserve(frameworkId: FrameworkID, jobId: UUID, reservationId: UUID, resources: Iterable[Resource]):
      Offer.Operation = {
    import scala.collection.JavaConverters._
    val reservedResources = resources.map { resource =>

      val reservation = Resource.ReservationInfo.newBuilder().
        setLabels(newLabels(
            Constants.FrameworkIdLabel -> frameworkId.getValue,
            Constants.ReservationIdLabel -> reservationId.toString,
            Constants.JobIdLabel -> jobId.toString)).
        setPrincipal(config.principal)

      Resource.newBuilder(resource).
        setRole(config.role).
        setReservation(reservation).
        build()
    }

    val reserve = Offer.Operation.Reserve.newBuilder().
      addAllResources(reservedResources.asJava).
      build()

    newOfferOperation(reserve)
  }

  def reserveAndCreateVolumes(
    frameworkId: FrameworkID,
    jobId: UUID,
    reservationId: UUID,
    resourceMatch: ResourceMatcher.ResourceMatch): List[Offer.Operation] = {

    val localVolumes = resourceMatch.matches.
      collect { case r: DiskResourceMatch =>
        r.consumed
      }.
      flatten.
      collect {
        case DiskResourceMatch.Consumption(_, _, _, source, Some(volume)) =>
          (source, volume)
      }.
      toList.
      distinct

    List(
      reserve(
        frameworkId,
        jobId = jobId,
        reservationId = reservationId,
        resources = resourceMatch.resources),
      createVolumes(
        frameworkId,
        jobId = jobId,
        reservationId = reservationId,
        localVolumes = localVolumes))
  }
}
