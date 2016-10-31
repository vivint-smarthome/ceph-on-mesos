package com.vivint.ceph

import akka.actor.{ Actor, ActorLogging, Cancellable, Kill, Stash }
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Sink
import akka.pattern.pipe
import com.vivint.ceph.kvstore.KVStore
import com.vivint.ceph.model.{ ReservationRelease, ReservationReleaseDetails }
import java.util.UUID
import scaldi.Injector
import scaldi.Injectable._
import scala.concurrent.duration._
import ProtoHelpers._
import akka.stream.scaladsl.Source
import java.time.ZonedDateTime
import lib.zonedDateTimeOrdering

object ReservationReaperActor {
  sealed trait Command
  case class OrderUnreserve(reservationId: UUID) extends Command

  case class UnknownReservation(reservationId: UUID, pendingOffer: PendingOffer) extends Command
  case object Cleanup extends Command
  case object GetPendingReleases extends Command
}

class ReservationReaperActor(implicit inj: Injector) extends Actor with ActorLogging with Stash {
  import ReservationReaperActor._

  val releaseStore = ReleaseStore(inject[KVStore])
  val offerOperations = inject[OfferOperations]
  val now = inject[() => ZonedDateTime]('now)
  val cleanupTimer = context.system.scheduler.schedule(1.day, 1.day, self, Cleanup)(context.dispatcher)
  case class InitialState(reelases: Seq[ReservationRelease])

  private var pendingReleases: Map[UUID, ReservationReleaseDetails] =
    Map.empty.withDefault { id => ReservationReleaseDetails(id, lastSeen = now()) }

  override def preStart(): Unit = {
    val timer = context.system.scheduler.scheduleOnce(15.seconds, self, 'timeout)(context.dispatcher)
    import context.dispatcher
    releaseStore.getReleases.map(InitialState(_)).pipeTo(self)
    context.become {
      case 'timeout =>
        log.error("Couldn't load state in time")
        context.stop(self)
      case InitialState(releases) =>
        log.info("loaded initial state")
        pendingReleases ++= releases.map { r => r.id -> r.toDetailed()}
        unstashAll()
        timer.cancel()
        context.become(receive)
      case _ =>
        stash()
    }
  }

  override def postStop(): Unit = {
    cleanupTimer.cancel()
  }

  def receive = {
    case UnknownReservation(reservationId, pendingOffer) =>
      val next = pendingReleases(reservationId).copy(
        lastSeen = now(),
        details = Some(pendingOffer.offer.toString))
      update(next)

      if (next.unreserve)
        pendingOffer.respond(offerOperations.unreserveOffer(pendingOffer.offer.resources))
      else
        pendingOffer.decline()

    case OrderUnreserve(reservationId) =>
      update(
        pendingReleases(reservationId).copy(lastSeen = now(), unreserve = true))

    case Cleanup =>
      val currentTime = now()
      val expireIfOlderThan = currentTime.minusDays(7L)
      for {
        (reservationId, pendingRelease) <- pendingReleases
        if pendingRelease.lastSeen isBefore expireIfOlderThan
      } {
        log.info(s"Cleaning up pending release for reservation last seen ${pendingRelease.lastSeen}")
        destroy(reservationId)
      }

    case GetPendingReleases =>
      sender ! pendingReleases.values.toList
  }

  def destroy(reservationId: UUID): Unit = {
    pendingReleases -= reservationId
    releaseStore.delete(reservationId)
  }

  def update(next: ReservationReleaseDetails): Unit = {
    val prior = pendingReleases(next.id)
    if (prior != next) {
      pendingReleases += prior.id -> next
      releaseStore.save(next.withoutDetails)
    }
  }
}
