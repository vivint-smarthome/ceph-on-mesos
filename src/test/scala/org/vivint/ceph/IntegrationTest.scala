package org.vivint.ceph

import akka.actor.{ ActorRef, ActorSystem, PoisonPill, Props }
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{ Flow, Keep, Sink }
import akka.testkit.{ ImplicitSender, TestProbe }
import akka.testkit.TestKit
import com.typesafe.config.{ Config, ConfigFactory, ConfigRenderOptions }
import java.io.File
import java.util.concurrent.atomic.AtomicInteger
import org.apache.commons.io.FileUtils
import org.apache.mesos.Protos
import org.scalatest.Inside
import org.scalatest.exceptions.TestFailedException
import org.scalatest.{ BeforeAndAfterAll, FunSpecLike, Matchers }
import mesosphere.mesos.protos.Resource.{CPUS, MEM, PORTS, DISK}
import org.vivint.ceph.kvstore.KVStore
import org.vivint.ceph.views.ConfigTemplates
import scala.annotation.tailrec
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.concurrent.duration._
import scala.concurrent.{ Await, Awaitable, ExecutionContext }
import scala.reflect.ClassTag
import scaldi.Injectable._
import scaldi.{ Injector, Module }

class IntegrationTest extends TestKit(ActorSystem("integrationTest"))
    with FunSpecLike with Matchers with BeforeAndAfterAll with Inside {

  val idx = new AtomicInteger()
  val fileStorePath = new File("tmp/test-store")
  import ProtoHelpers._

  def await[T](f: Awaitable[T], duration: FiniteDuration = 5.seconds) = {
    Await.result(f, duration)
  }

  def renderConfig(cfg: Config) = {
    cfg.root().render(ConfigRenderOptions.defaults.setOriginComments(false))
  }

  override def beforeAll(): Unit = {
    FileUtils.deleteDirectory(fileStorePath)
  }

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
  }

  class TestBindings extends Module {
    val id = idx.incrementAndGet()
    bind [TestProbe] to { TestProbe() } destroyWith {
      _.ref ! PoisonPill
    }

    bind [ActorRef] identifiedBy(classOf[FrameworkActor]) to { inject[TestProbe].ref }
    bind [ActorRef] identifiedBy(classOf[TaskActor]) to {
      system.actorOf(Props(new TaskActor), s"task-actor-${id}")
    } destroyWith { _ ! PoisonPill }

    // bind [KVStore] to { new kvstore.FileStore(new File(fileStorePath, idx.incrementAndGet.toString)) }
    bind [KVStore] to { new kvstore.MemStore }
    bind [OfferOperations] to new OfferOperations
    bind [FrameworkIdStore] to new FrameworkIdStore
    bind [ConfigTemplates] to new ConfigTemplates
    bind [ActorSystem] to system
    bind [AppConfiguration] to {
      AppConfiguration(master = "hai", name = "ceph-test", principal = "ceph", secret = None, role = "ceph",
        zookeeper = "zk://test", offerTimeout = 5.seconds, publicNetwork = "10.11.12.0/24",
        clusterNetwork =  "10.11.12.0/24", storageBackend = "memory")
    }
    bind [String => String] identifiedBy 'ipResolver to { _: String =>
      "10.11.12.1"
    }
  }

  def cephConfUpdates(implicit inj: Injector) = {
    inject[KVStore].watch("ceph.conf").
      map { _.map { bytes => ConfigFactory.parseString(new String(bytes)) } }.
      collect { case Some(cfg) => cfg }
  }

  def storeConfig(implicit inj: Injector) =
    Flow[Config].
      mapAsync(1) { cfg =>
        inject[KVStore].set("ceph.conf", renderConfig(cfg).getBytes)
      }.toMat(Sink.ignore)(Keep.right)

  def updateConfig(newCfg: String)(implicit inj: Injector) = {
    implicit val materializer = ActorMaterializer()
    await {
      cephConfUpdates.
        take(1).
        map { cfg =>
          ConfigFactory.parseString(newCfg).withFallback(cfg)
        }.
        runWith(storeConfig)
    }
  }

  @tailrec final def gatherResponses(testProbe: TestProbe, offers: List[Protos.Offer],
    results: Map[Protos.Offer, FrameworkActor.OfferResponseCommand] = Map.empty,
    ignore: PartialFunction[Any, Boolean] = { case _ => false }):
      Map[Protos.Offer, FrameworkActor.OfferResponseCommand] = {
    if (offers.isEmpty)
      results
    else {
      val received = testProbe.receiveOne(5.seconds)
      if (ignore.isDefinedAt(received) && ignore(received))
        gatherResponses(testProbe, offers, results, ignore)
      else {
        val (newResults, newOffers) = inside(received) {
          case msg: FrameworkActor.OfferResponseCommand =>
            val offer = offers.find(_.getId == msg.offerId).get
            ( results.updated(offer, msg),
              offers.filterNot(_ == offer))
        }
        gatherResponses(testProbe, newOffers, newResults, ignore)
      }
    }
  }

  def gatherResponse(testProbe: TestProbe, offer: Protos.Offer, ignore: PartialFunction[Any, Boolean] = { case _ => false }):
      FrameworkActor.OfferResponseCommand = {
    gatherResponses(testProbe, List(offer), ignore = ignore)(offer)
  }


  def reservationResponse(offer: Protos.Offer): PartialFunction[Any, Protos.Offer] = {
    case offerResponse: FrameworkActor.AcceptOffer =>
      offerResponse.offerId shouldBe offer.getId
      val List(reserve, create) = offerResponse.operations
      reserve.hasReserve shouldBe (true)
      create.hasCreate shouldBe (true)

      MesosTestHelper.mergeReservation(
        offer, reserve.getReserve, create.getCreate)
  }

  it("should launch a monitors on unique hosts") {
    implicit val ec: ExecutionContext = SameThreadExecutionContext
    val module = new TestBindings
    import module.injector

    val taskActor = inject[ActorRef](classOf[TaskActor])
    val probe = inject[TestProbe]
    implicit val sender = probe.ref

    inject[FrameworkIdStore].set(MesosTestHelper.frameworkID)

    val kvStore = inject[KVStore]
    val configStore = new ConfigStore(inject[KVStore])
    implicit val materializer = ActorMaterializer()

    // Wait for configuration update
    val config = await(cephConfUpdates.runWith(Sink.head))

    updateConfig("deployment.mon.count = 2")

    probe.receiveOne(5.seconds) shouldBe FrameworkActor.ReviveOffers

    val offer = MesosTestHelper.makeBasicOffer(slaveId = 0).build
    val sameOffer = MesosTestHelper.makeBasicOffer(slaveId = 0).build

    // Send an offer!
    taskActor ! FrameworkActor.ResourceOffers(List(offer, sameOffer))

    val responses = gatherResponses(probe, List(offer, sameOffer))

    val reservedOffer = inside(responses(offer))(reservationResponse(offer))

    inside(responses(sameOffer)) {
      case offerResponse: FrameworkActor.DeclineOffer =>
        offerResponse.offerId shouldBe sameOffer.getId
    }

    // Send the reservation
    taskActor ! FrameworkActor.ResourceOffers(List(sameOffer))
    probe.receiveOne(5.seconds) shouldBe a[FrameworkActor.DeclineOffer]

    taskActor ! FrameworkActor.ResourceOffers(List(reservedOffer))

    val taskId = inside(probe.receiveOne(5.seconds)) {
      case offerResponse: FrameworkActor.AcceptOffer =>
        offerResponse.offerId shouldBe reservedOffer.getId
        offerResponse.operations(0).getType shouldBe Protos.Offer.Operation.Type.LAUNCH
        val List(task) = offerResponse.operations(0).getLaunch.tasks
        task.getCommand.getValue.contains("entrypoint.sh") shouldBe true
        task.getTaskId
    }

    // And, again, let's make sure it rejects it
    taskActor ! FrameworkActor.ResourceOffers(List(sameOffer))
    probe.receiveOne(5.seconds) shouldBe a[FrameworkActor.DeclineOffer]

    // Now let's set the offer to running
    taskActor ! FrameworkActor.StatusUpdate(newTaskStatus(
      taskId = taskId.getValue,
      slaveId = reservedOffer.getSlaveId.getValue,
      state = Protos.TaskState.TASK_RUNNING))


    // Now, let's make an offer for the second one!
    val altSlaveOffer = MesosTestHelper.makeBasicOffer(slaveId = 1).build
    taskActor ! FrameworkActor.ResourceOffers(List(altSlaveOffer))

    val altReservedOffer = inside(probe.receiveOne(5.seconds))(reservationResponse(altSlaveOffer))

    taskActor ! FrameworkActor.ResourceOffers(List(altReservedOffer))

    val ignoreRevive: PartialFunction[Any,Boolean] = { case FrameworkActor.ReviveOffers => true }
    inside(gatherResponse(probe, altReservedOffer, ignoreRevive)) {
      case offerResponse: FrameworkActor.AcceptOffer =>
        offerResponse.offerId shouldBe altReservedOffer.getId
        offerResponse.operations(0).getType shouldBe Protos.Offer.Operation.Type.LAUNCH
        val List(task) = offerResponse.operations(0).getLaunch.tasks
        val shCommand = task.getCommand.getValue
        shCommand.contains("entrypoint.sh") shouldBe true
        shCommand.contains("ceph mon getmap") shouldBe true
    }

    module.destroy(_ => true)
  }
}
