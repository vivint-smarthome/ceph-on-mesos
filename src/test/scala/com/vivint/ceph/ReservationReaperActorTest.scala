package com.vivint.ceph

import akka.actor.{ ActorRef, ActorSystem, Props }
import akka.pattern.ask
import akka.util.Timeout
import akka.testkit.{ TestKit, TestProbe }
import com.vivint.ceph.ReservationReaperActor._
import com.vivint.ceph.kvstore.KVStore
import com.vivint.ceph.model.{ ReservationRelease, ReservationReleaseDetails }
import java.time.ZonedDateTime
import java.util.UUID
import org.scalatest.{ BeforeAndAfterAll, Inside }
import org.scalatest.path.FunSpecLike
import org.scalatest.{FunSpec, Matchers}
import scala.concurrent.duration._
import scaldi.{ Injector, Module }
import scaldi.Injectable._
import ProtoHelpers._

class ReservationReaperActorTest extends lib.CephActorTest("releaseActorTest")
    with FunSpecLike with Matchers with BeforeAndAfterAll with Inside with lib.TestHelpers {

  class TestBindings extends Module {
    val id = idx.incrementAndGet()

    var now: ZonedDateTime = ZonedDateTime.parse("2016-01-01T00:00:00Z")
    bind [TestProbe] to { TestProbe() }
    bind [AppConfiguration] to Workbench.newAppConfiguration()
    bind [OfferOperations] to { new OfferOperations }
    bind [KVStore] to { new kvstore.MemStore }
    bind [ActorRef] identifiedBy(classOf[ReservationReaperActor]) to {
      system.actorOf(Props(new ReservationReaperActor), s"release-actor-${id}")
    } destroyWith (system.stop)
    bind [() => ZonedDateTime] identifiedBy 'now to { () => now }
  }

  def getReleases(implicit inj: Injector) = {
    val releaseActor = inject[ActorRef](classOf[ReservationReaperActor])
    await((releaseActor ? GetPendingReleases).mapTo[List[ReservationReleaseDetails]])
  }

  trait Fixtures {
    val module = new TestBindings
    implicit val injector = module.injector

    val probe = inject[TestProbe]
    implicit val sender = probe.ref
    lazy val releaseActor = inject[ActorRef](classOf[ReservationReaperActor])
  }

  implicit val timeout = Timeout(5.seconds)

  def reservedOffer(reservationId: UUID) =
    MesosTestHelper.
      makeBasicOffer(
        role = "ceph",
        reservationLabels = Some(
          newLabels(Constants.ReservationIdLabel -> reservationId.toString))).
      build

  describe("ReservationReaperActor") {
    it("does not register an item for release until it is permitted to do so") {
      new Fixtures {
        val reservationId = UUID.randomUUID()

        val pendingOffer = PendingOffer(
          reservedOffer(reservationId))

        releaseActor ! UnknownReservation(reservationId, pendingOffer)
        await(pendingOffer.resultingOperations) shouldBe Nil
        val List(release) = await((releaseActor ? GetPendingReleases).mapTo[List[ReservationReleaseDetails]])

        release.unreserve shouldBe false
        release.lastSeen shouldBe module.now
        release.details.nonEmpty shouldBe true
      }
    }

    it("releases an offer which is permitted to be released ") {
      new Fixtures {
        val reservationId = UUID.randomUUID()

        val pendingOffer = PendingOffer(
          reservedOffer(reservationId))

        releaseActor ! OrderRelease(reservationId)
        releaseActor ! UnknownReservation(reservationId, pendingOffer)
        await(pendingOffer.resultingOperations).nonEmpty shouldBe true
      }
    }

    it("loads the initial state from the kvStore") {
      new Fixtures {
        val releaseStore = ReleaseStore(inject[KVStore])
        val reservationId = UUID.randomUUID()
        releaseStore.save(ReservationRelease(reservationId, unreserve = true, lastSeen = module.now))

        val List(release) = getReleases
        release.id shouldBe reservationId
        release.unreserve shouldBe true
      }
    }

    it("cleans up pending releases after 7 days") {
      new Fixtures {
        val releaseStore = ReleaseStore(inject[KVStore])
        val reservationId = UUID.randomUUID()
        releaseStore.save(ReservationRelease(reservationId, unreserve = true, lastSeen = module.now))

        releaseActor ! Cleanup
        // Shouldn't have an effect
        inside(getReleases) {
          case List(release) =>
            release.id shouldBe reservationId
            release.unreserve shouldBe true
        }
        inside(await(releaseStore.getReleases)) {
          case List(release) =>
            release.id shouldBe reservationId
            release.unreserve shouldBe true
        }

        // fast forward 8 days and run cleanup again
        module.now = module.now.plusDays(8L)

        releaseActor ! Cleanup

        getReleases.isEmpty shouldBe true
        await(releaseStore.getReleases).isEmpty shouldBe true
      }
    }
  }
}
