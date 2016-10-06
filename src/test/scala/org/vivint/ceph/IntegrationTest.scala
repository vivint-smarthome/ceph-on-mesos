package org.vivint.ceph

import akka.actor.{ ActorRef, ActorSystem, PoisonPill, Props }
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{ Flow, Keep, Sink }
import akka.testkit.TestKit
import akka.testkit.TestProbe
import com.typesafe.config.{ Config, ConfigFactory, ConfigRenderOptions }
import java.io.File
import java.nio.charset.StandardCharsets.UTF_8
import java.util.Base64.getDecoder
import java.util.UUID
import java.util.concurrent.atomic.AtomicInteger
import mesosphere.mesos.protos.Resource.{CPUS, MEM, PORTS, DISK}
import org.apache.commons.io.FileUtils
import org.apache.mesos.Protos
import org.scalatest.Inside
import org.scalatest.exceptions.TestFailedException
import org.scalatest.{ BeforeAndAfterAll, FunSpecLike, Matchers }
import org.vivint.ceph.kvstore.KVStore
import org.vivint.ceph.lib.TgzHelper
import org.vivint.ceph.model.{ CephNode, NodeRole, RunState, ServiceLocation }
import org.vivint.ceph.views.ConfigTemplates
import scala.annotation.tailrec
import scala.collection.breakOut
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

  trait TestBindings extends Module {
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


  def handleReservationResponse(offer: Protos.Offer): PartialFunction[Any, Protos.Offer] = {
    case offerResponse: FrameworkActor.AcceptOffer =>
      offerResponse.offerId shouldBe offer.getId
      val List(reserve, create) = offerResponse.operations
      reserve.hasReserve shouldBe (true)
      create.hasCreate shouldBe (true)

      MesosTestHelper.mergeReservation(
        offer, reserve.getReserve, create.getCreate)
  }
  val ignoreRevive: PartialFunction[Any,Boolean] = { case FrameworkActor.ReviveOffers => true }

  it("should launch a monitors on unique hosts") {
    implicit val ec: ExecutionContext = SameThreadExecutionContext
    val module = new TestBindings {}
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

    val reservedOffer = inside(responses(offer))(handleReservationResponse(offer))

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
        val shCommand = task.getCommand.getValue
        shCommand.contains("entrypoint.sh") shouldBe true
        shCommand.contains("ceph mon getmap") shouldBe false
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

    val altReservedOffer = inside(probe.receiveOne(5.seconds))(handleReservationResponse(altSlaveOffer))

    taskActor ! FrameworkActor.ResourceOffers(List(altReservedOffer))

    inside(gatherResponse(probe, altReservedOffer, ignoreRevive)) {
      case offerResponse: FrameworkActor.AcceptOffer =>
        offerResponse.operations(0).getType shouldBe Protos.Offer.Operation.Type.LAUNCH
        val List(task) = offerResponse.operations(0).getLaunch.tasks
        val shCommand = task.getCommand.getValue
        shCommand.contains("entrypoint.sh") shouldBe true
        shCommand.contains("ceph mon getmap") shouldBe true
    }

    module.destroy(_ => true)
  }

  def readConfig(configMD5: String): Map[String, String] =
    TgzHelper.readTgz(getDecoder.decode(configMD5)).map {
      case (k, v) => (k, new String(v, UTF_8))
    }.toMap

  it("should launch OSDs") {
    implicit val ec: ExecutionContext = SameThreadExecutionContext
    val monLocation = ServiceLocation(hostname = "slave-12", ip = "10.11.12.12", port = 30125)
    val monitorNode = CephNode(
      id = UUID.randomUUID(),
      cluster = "ceph",
      role = NodeRole.Monitor,
      lastLaunched = Some(RunState.Running),
      reservationConfirmed = true,
      slaveId = Some("slave-12"),
      location = Some(monLocation))

    val module = new TestBindings {
      bind [KVStore] to {
        val store = new kvstore.MemStore
        val taskStore = new TaskStore(store)
        taskStore.save(monitorNode)
        store
      }
    }

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

    inside(probe.receiveOne(5.seconds)) {
      case r: FrameworkActor.Reconcile =>
        r.tasks.length shouldBe 1
        val taskStatus = r.tasks.head
        taskActor ! FrameworkActor.StatusUpdate(taskStatus.toBuilder.setState(Protos.TaskState.TASK_RUNNING).build)
    }
    updateConfig("deployment.osd.count = 1")
    probe.receiveOne(5.seconds) shouldBe FrameworkActor.ReviveOffers

    val offer = MesosTestHelper.makeBasicOffer(slaveId = 0).
      addResources(newScalarResource(
        DISK, 1024000.0, disk = Some(MesosTestHelper.mountDisk("/mnt/ssd-1")))).build
    taskActor ! FrameworkActor.ResourceOffers(List(offer))

    val reservedOffer = inside(gatherResponse(probe, offer, ignoreRevive))(handleReservationResponse(offer))

    taskActor ! FrameworkActor.ResourceOffers(List(reservedOffer))

    inside(gatherResponse(probe, reservedOffer, ignoreRevive)) {
      case offerResponse: FrameworkActor.AcceptOffer =>
        offerResponse.operations(0).getType shouldBe Protos.Offer.Operation.Type.LAUNCH
        val List(task) = offerResponse.operations(0).getLaunch.tasks
        val shCommand = task.getCommand.getValue
        shCommand.contains("entrypoint.sh osd_directory") shouldBe true
        val config = readConfig(task.getCommand.getEnvironment.get("CEPH_CONFIG_TGZ").get)
        val cephConfig = config("etc/ceph/ceph.conf")
        cephConfig.lines.filter(_.contains("ms_bind_port")).toList shouldBe List(
          "ms_bind_port_min = 31000",
          "ms_bind_port_max = 31004")
        cephConfig.lines.filter(_.startsWith("mon")).take(3).toList shouldBe List(
          s"mon initial members = ${monLocation.hostname}",
          s"mon host = ${monLocation.hostname}:${monLocation.port}",
          s"mon addr = ${monLocation.ip}:${monLocation.port}")
    }
  }
}
