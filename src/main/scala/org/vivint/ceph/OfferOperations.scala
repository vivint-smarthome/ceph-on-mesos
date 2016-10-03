package org.vivint.ceph

import akka.actor.{ Actor, ActorContext, ActorLogging, ActorRef, Cancellable, FSM, Kill, Props, Stash }
import akka.pattern.pipe
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{ Flow, Keep, Sink }
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
      filter { r => r.hasDisk && r.getDisk.hasPersistence }.
      map { r =>
        val b = r.toBuilder.clearDisk
        r.diskSourceOption.foreach { source =>
          b.setDisk(ProtoHelpers.newDisk(Some(source)))
        }
        b.build
      }

    val offersToRelease = resources.
      filter(_.hasReservation)

    Seq(
      newOfferOperation(
        destroy = Some(newDestroyOperation(volumesToDestroy))),
      newOfferOperation(
        unreserve = Some(newUnreserveOperation(resources))))
  }

  def volId(taskId: String, containerPath: String): String =
    taskId + "#" + containerPath

  /** Adapated from Marathon code.
    * TODO - put appropriate copyright information
    */
  def createVolumes(
    frameworkId: String,
    taskId: String,
    localVolumes: Iterable[(DiskSource, PersistentVolume)]): Offer.Operation = {
    import scala.collection.JavaConverters._

    val volumes: Iterable[Resource] = localVolumes.map {
      case (source, vol) =>
        val disk = {
          val persistence = Resource.DiskInfo.Persistence.newBuilder().
            setId(volId(taskId = taskId, containerPath = vol.containerPath)).
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
            Constants.FrameworkIdLabel -> frameworkId,
            Constants.TaskIdLabel -> taskId)).
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

    Offer.Operation.newBuilder.
      setType(Offer.Operation.Type.CREATE).
      setCreate(create).
      build()
  }

  /** Adapated from Marathon code.
    * TODO - put appropriate copyright information */
  def reserve(frameworkId: String, taskId: String, resources: Iterable[Resource]): Offer.Operation = {
    import scala.collection.JavaConverters._
    val reservedResources = resources.map { resource =>

      val reservation = Resource.ReservationInfo.newBuilder().
        setLabels(newLabels(
            Constants.FrameworkIdLabel -> frameworkId,
            Constants.TaskIdLabel -> taskId)).
        setPrincipal(config.principal)

      Resource.newBuilder(resource).
        setRole(config.role).
        setReservation(reservation).
        build()
    }

    val reserve = Offer.Operation.Reserve.newBuilder().
      addAllResources(reservedResources.asJava).
      build()

    Offer.Operation.newBuilder().
      setType(Offer.Operation.Type.RESERVE).
      setReserve(reserve).
      build()
  }

  def reserveAndCreateVolumes(
    frameworkId: String,
    taskId: String,
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
      reserve(frameworkId, taskId, resourceMatch.resources),
      createVolumes(frameworkId, taskId, localVolumes))
  }
}
