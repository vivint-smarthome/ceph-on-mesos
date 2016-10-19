package com.vivint.ceph

import akka.actor.{ ActorRef, ActorSystem, PoisonPill, Props }
import akka.pattern.ask
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{ Flow, Keep, Sink }
import akka.testkit.TestKit
import akka.testkit.TestProbe
import akka.util.Timeout
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
import com.vivint.ceph.kvstore.KVStore
import com.vivint.ceph.lib.TgzHelper
import com.vivint.ceph.model.{ PersistentState, JobRole, Job, RunState, ServiceLocation }
import com.vivint.ceph.views.ConfigTemplates
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
  implicit val timeout = Timeout(3.seconds)
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
    bind [AppConfiguration] to Workbench.newAppConfiguration()

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

  @tailrec final def receiveIgnoring(probe: TestProbe, d: FiniteDuration, ignore: PartialFunction[Any, Boolean]):
      Option[Any] = {
    val received = probe.receiveOne(d)

    if (ignore.isDefinedAt(received) && ignore(received))
      receiveIgnoring(probe, d, ignore)
    else
      Option(received)
  }

  @tailrec final def gatherResponses(testProbe: TestProbe, offers: List[Protos.Offer],
    results: Map[Protos.Offer, FrameworkActor.OfferResponseCommand] = Map.empty,
    ignore: PartialFunction[Any, Boolean] = { case _ => false }):
      Map[Protos.Offer, FrameworkActor.OfferResponseCommand] = {
    if (offers.isEmpty)
      results
    else {
      val received = receiveIgnoring(testProbe, 5.seconds, ignore)
      val (newResults, newOffers) = inside(received) {
        case Some(msg: FrameworkActor.OfferResponseCommand) =>
          val offer = offers.find(_.getId == msg.offerId).get
          ( results.updated(offer, msg),
            offers.filterNot(_ == offer))
      }
      gatherResponses(testProbe, newOffers, newResults, ignore)
    }
  }

  def gatherResponse(testProbe: TestProbe, offer: Protos.Offer, ignore: PartialFunction[Any, Boolean] = { case _ => false }):
      FrameworkActor.OfferResponseCommand = {
    gatherResponses(testProbe, List(offer), ignore = ignore)(offer)
  }

  def handleGetCreateReserve:
      PartialFunction[Option[Any], (Protos.Offer.Operation.Reserve, Protos.Offer.Operation.Create)] = {
    case Some(offerResponse: FrameworkActor.AcceptOffer) =>
      val List(reserve, create) = offerResponse.operations
      reserve.hasReserve shouldBe (true)
      create.hasCreate shouldBe (true)
      (reserve.getReserve, create.getCreate)
  }

  def handleReservationResponse(offer: Protos.Offer):
      PartialFunction[Option[Any], Protos.Offer] = handleGetCreateReserve.andThen {
    case (reserve, create) =>
      MesosTestHelper.mergeReservation(offer, reserve, create)
  }

  val ignoreRevive: PartialFunction[Any,Boolean] = { case FrameworkActor.ReviveOffers => true }

  def readConfig(configMD5: String): Map[String, String] =
    TgzHelper.readTgz(getDecoder.decode(configMD5)).map {
      case (k, v) => (k, new String(v, UTF_8))
    }.toMap


  def getTasks(implicit inj: Injector) = {
    await((inject[ActorRef](classOf[TaskActor]) ? TaskActor.GetJobs).mapTo[Map[UUID, Job]])
  }

  @tailrec final def pollTasks(duration: FiniteDuration, frequency: FiniteDuration = 50.millis)(
    poller: Map[UUID, Job] => Boolean)(implicit inj: Injector): Map[UUID, Job] = {
    val before = System.currentTimeMillis()
    val tasks = getTasks
    if (poller(tasks))
      tasks
    else if (duration < 0.millis) {
      fail("Predicate for poll tasks didn't match in time")
    } else {
      val surpassed = System.currentTimeMillis() - before
      Thread.sleep(Math.max(0, frequency.toMillis - surpassed))
      val totalSurpassed = System.currentTimeMillis() - before
      pollTasks(duration - totalSurpassed.millis, frequency)(poller)
    }
  }

  trait OneMonitorRunning {
    implicit val ec: ExecutionContext = SameThreadExecutionContext
    val monLocation = ServiceLocation(hostname = "slave-12", ip = "10.11.12.12", port = 30125)
    val cluster = "ceph"
    val monitorTaskId = model.Job.makeTaskId(JobRole.Monitor, cluster)
    val monitorTask = Workbench.newRunningMonitorJob(
      taskId = monitorTaskId,
      location = monLocation)

    val module = new TestBindings {
      bind [KVStore] to {
        val store = new kvstore.MemStore
        val taskStore = new JobStore(store)
        taskStore.save(monitorTask)
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

    inside(Option(probe.receiveOne(5.seconds))) {
      case Some(r: FrameworkActor.Reconcile) =>
        r.tasks.length shouldBe 1
        val taskStatus = r.tasks.head
        taskActor ! FrameworkActor.StatusUpdate(taskStatus.toBuilder.setState(Protos.TaskState.TASK_RUNNING).build)
    }

    probe.receiveOne(5.seconds) shouldBe (FrameworkActor.Reconcile(Nil))
  }

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

    probe.receiveOne(5.seconds) shouldBe FrameworkActor.Reconcile(Nil)
    probe.receiveOne(5.seconds) shouldBe FrameworkActor.ReviveOffers

    val offer = MesosTestHelper.makeBasicOffer(slaveId = 0).build
    val sameOffer = MesosTestHelper.makeBasicOffer(slaveId = 0).build

    // Send an offer!
    taskActor ! FrameworkActor.ResourceOffers(List(offer, sameOffer))

    val responses = gatherResponses(probe, List(offer, sameOffer))

    val reservedOffer = inside(responses.get(offer))(handleReservationResponse(offer))

    inside(responses(sameOffer)) {
      case offerResponse: FrameworkActor.DeclineOffer =>
        offerResponse.offerId shouldBe sameOffer.getId
    }

    // Send the reservation
    taskActor ! FrameworkActor.ResourceOffers(List(sameOffer))
    probe.receiveOne(5.seconds) shouldBe a[FrameworkActor.DeclineOffer]

    taskActor ! FrameworkActor.ResourceOffers(List(reservedOffer))

    val taskId = inside(Option(probe.receiveOne(5.seconds))) {
      case Some(offerResponse: FrameworkActor.AcceptOffer) =>
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

    val altReservedOffer = inside(Option(probe.receiveOne(5.seconds)))(handleReservationResponse(altSlaveOffer))

    taskActor ! FrameworkActor.ResourceOffers(List(altReservedOffer))

    inside(Option(gatherResponse(probe, altReservedOffer, ignoreRevive))) {
      case Some(offerResponse: FrameworkActor.AcceptOffer) =>
        offerResponse.operations(0).getType shouldBe Protos.Offer.Operation.Type.LAUNCH
        val List(task) = offerResponse.operations(0).getLaunch.tasks
        val shCommand = task.getCommand.getValue
        shCommand.contains("entrypoint.sh") shouldBe true
        shCommand.contains("ceph mon getmap") shouldBe true
    }

    module.destroy(_ => true)
  }

  it("should launch OSDs") {
    new OneMonitorRunning {
      import module.injector
      updateConfig("deployment.osd.count = 1")
      probe.receiveOne(5.seconds) shouldBe FrameworkActor.ReviveOffers

      pollTasks(5.seconds) { _.values.exists { job => job.role == JobRole.OSD && job.wantingNewOffer } }

      val offer = MesosTestHelper.makeBasicOffer(slaveId = 0).
        addResources(newScalarResource(
          DISK, 1024000.0, disk = Some(MesosTestHelper.mountDisk("/mnt/ssd-1")))).build

      taskActor ! FrameworkActor.ResourceOffers(List(offer))

      val reservedOffer = inside(Option(gatherResponse(probe, offer, ignoreRevive)))(handleReservationResponse(offer))

      reservedOffer.resources.filter(_.getName == DISK).head.getScalar.getValue shouldBe 1024000.0

      taskActor ! FrameworkActor.ResourceOffers(List(reservedOffer))

      inside(gatherResponse(probe, reservedOffer, ignoreRevive)) {
        case offerResponse: FrameworkActor.AcceptOffer =>
          offerResponse.operations(0).getType shouldBe Protos.Offer.Operation.Type.LAUNCH
          val List(task) = offerResponse.operations(0).getLaunch.tasks
          val shCommand = task.getCommand.getValue
          shCommand.contains("entrypoint.sh osd_directory") shouldBe true
          val config = readConfig(task.getCommand.environment("CEPH_CONFIG_TGZ"))
          val cephConfig = config("etc/ceph/ceph.conf")
          cephConfig.lines.filter(_.contains("ms_bind_port")).toList shouldBe List(
            "ms_bind_port_min = 31000",
            "ms_bind_port_max = 31004")
          cephConfig.lines.filter(_.startsWith("mon")).take(2).toList shouldBe List(
            s"mon initial members = ${monLocation.hostname}",
            s"mon host = ${monLocation.ip}:${monLocation.port}")
      }
    }
  }

  it("should kill a task in response to a goal change") {
    new OneMonitorRunning {
      import module.injector

      taskActor ! TaskActor.UpdateGoal(monitorTask.id, model.RunState.Paused)

      inside(receiveIgnoring(probe, 5.seconds, ignoreRevive)) {
        case Some(FrameworkActor.KillTask(taskId)) =>
          taskId.getValue shouldBe monitorTaskId
      }

      taskActor ! FrameworkActor.StatusUpdate(
        newTaskStatus(monitorTaskId, monitorTask.slaveId.get, state = Protos.TaskState.TASK_KILLED))

      val offerOperations = inject[OfferOperations]
      val reservedOffer = MesosTestHelper.makeBasicOffer(
        slaveId = 12,
        role = "ceph",
        reservationLabels = Some(newLabels(
          Constants.FrameworkIdLabel -> MesosTestHelper.frameworkID.getValue,
          Constants.ReservationIdLabel -> monitorTask.reservationId.get.toString,
          Constants.JobIdLabel -> monitorTask.id.toString))).
        build

      taskActor ! FrameworkActor.ResourceOffers(List(reservedOffer))

      val relaunchedTaskId = inside(gatherResponse(probe, reservedOffer, ignoreRevive)) {
        case offerResponse: FrameworkActor.AcceptOffer =>
          offerResponse.operations(0).getType shouldBe Protos.Offer.Operation.Type.LAUNCH
          val List(task) = offerResponse.operations(0).getLaunch.tasks
          val shCommand = task.getCommand.getValue
          shCommand.contains("sleep ") shouldBe true
          task.getTaskId.getValue
      }

      taskActor ! TaskActor.UpdateGoal(monitorTask.id, model.RunState.Running)

      taskActor ! FrameworkActor.StatusUpdate(
        newTaskStatus(relaunchedTaskId, monitorTask.slaveId.get, state = Protos.TaskState.TASK_RUNNING))

      inside(receiveIgnoring(probe, 5.seconds, ignoreRevive)) {
        case Some(FrameworkActor.KillTask(taskId)) =>
          taskId.getValue shouldBe relaunchedTaskId
      }

      taskActor ! FrameworkActor.StatusUpdate(
        newTaskStatus(relaunchedTaskId, monitorTask.slaveId.get, state = Protos.TaskState.TASK_KILLED))

      taskActor ! FrameworkActor.ResourceOffers(List(reservedOffer))

      inside(gatherResponse(probe, reservedOffer, ignoreRevive)) {
        case offerResponse: FrameworkActor.AcceptOffer =>
          offerResponse.operations(0).getType shouldBe Protos.Offer.Operation.Type.LAUNCH
          val List(task) = offerResponse.operations(0).getLaunch.tasks
          val shCommand = task.getCommand.getValue
          shCommand.contains("entrypoint.sh mon") shouldBe true
      }
    }
  }

  it("should require that the first monitor is running before launching another") {
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

    probe.receiveOne(5.seconds) shouldBe a[FrameworkActor.Reconcile]

    updateConfig("deployment.mon.count = 2")

    probe.receiveOne(5.seconds) shouldBe FrameworkActor.ReviveOffers

    val offers = List(
      MesosTestHelper.makeBasicOffer(slaveId = 0).build,
      MesosTestHelper.makeBasicOffer(slaveId = 1).build)

    // Send an offer!
    taskActor ! FrameworkActor.ResourceOffers(offers)

    val responses = gatherResponses(probe, offers, ignore = ignoreRevive)

    val List(reservationOffer, reservationOffer2) = responses.map { case (offer, response) =>
      inside(Option(response))(handleReservationResponse(offer))
    }.toList

    taskActor ! FrameworkActor.ResourceOffers(List(reservationOffer, reservationOffer2))

    val reservedResponses = gatherResponses(probe, List(reservationOffer, reservationOffer2), ignore = ignoreRevive)

    val launchedTaskId = inside(reservedResponses(reservationOffer)) {
      case offerResponse: FrameworkActor.AcceptOffer =>
        offerResponse.operations(0).getType shouldBe Protos.Offer.Operation.Type.LAUNCH
        val List(task) = offerResponse.operations(0).getLaunch.tasks
        val shCommand = task.getCommand.getValue
        shCommand.contains("entrypoint.sh mon") shouldBe true
        shCommand.contains("ceph mon getmap") shouldBe false
        task.getTaskId.getValue
    }

    inside(reservedResponses(reservationOffer2)) {
      case offerResponse: FrameworkActor.DeclineOffer =>
        ()
    }

    val (Seq(launchedTask), Seq(unlaunchedTask)) = await((taskActor ? TaskActor.GetJobs).mapTo[Map[UUID, Job]]).
      values.
      partition(_.taskId == Some(launchedTaskId))

    unlaunchedTask.behavior.name shouldBe ("WaitForGoal")

    taskActor ! FrameworkActor.StatusUpdate(
      newTaskStatus(launchedTask.taskId.get, launchedTask.slaveId.get, Protos.TaskState.TASK_RUNNING))

    taskActor ! FrameworkActor.ResourceOffers(List(reservationOffer2))

    inside(gatherResponse(probe, reservationOffer2, ignoreRevive)) {
      case offerResponse: FrameworkActor.AcceptOffer =>
        offerResponse.operations(0).getType shouldBe Protos.Offer.Operation.Type.LAUNCH
        val List(task) = offerResponse.operations(0).getLaunch.tasks
        val shCommand = task.getCommand.getValue
        shCommand.contains("entrypoint.sh mon") shouldBe true
        shCommand.contains("ceph mon getmap") shouldBe true
    }
  }

  it("should launch RGW tasks with provided docker settings, and keep the RGW task up") {
    new OneMonitorRunning {
      import module.injector

      updateConfig("""
        |deployment.rgw {
        |  count = 1
        |  cpus = 0.5
        |  mem = 128
        |  port = 80
        |  docker_args {
        |    hostname = "le-docker.host.rgw"
        |    network = "weave"
        |  }
        |}
        |""".stripMargin)

      probe.receiveOne(5.seconds) shouldBe FrameworkActor.ReviveOffers
      val offer = MesosTestHelper.makeBasicOffer(slaveId = 0).build

      pollTasks(5.seconds) { _.values.exists { job => job.role == JobRole.RGW && job.wantingNewOffer } }

      taskActor ! FrameworkActor.ResourceOffers(List(offer))

      val (rgwJobId, launchedTaskId) = inside(gatherResponse(probe, offer, ignoreRevive)) {
        case offerResponse: FrameworkActor.AcceptOffer =>
          val List(launchOp) = offerResponse.operations.toList
          launchOp.getType shouldBe Protos.Offer.Operation.Type.LAUNCH
          val List(task) = launchOp.getLaunch.tasks
          task.getResourcesList.filter(_.getName == CPUS)(0).getScalar.getValue shouldBe 0.5
          task.getResourcesList.filter(_.getName == MEM)(0).getScalar.getValue shouldBe 128.0

          val shCommand = task.getCommand.getValue
          val dockerParams = task.getContainer.getDocker.params
          dockerParams("hostname") shouldBe ("le-docker.host.rgw")
          dockerParams("network") shouldBe ("weave")
          task.getCommand.environment("RGW_CIVETWEB_PORT") shouldBe ("80")
          shCommand.contains("entrypoint.sh rgw") shouldBe true
          (UUID.fromString(task.getLabels.get(Constants.JobIdLabel).get), task.getTaskId.getValue)
      }

      taskActor ! FrameworkActor.StatusUpdate(
        newTaskStatus(launchedTaskId, offer.getSlaveId.getValue, Protos.TaskState.TASK_RUNNING))

      taskActor ! FrameworkActor.StatusUpdate(
        newTaskStatus(launchedTaskId, offer.getSlaveId.getValue, Protos.TaskState.TASK_FAILED))

      val rgwJobAfterLaunch = getTasks.apply(rgwJobId)

      rgwJobAfterLaunch.behavior.name shouldBe "MatchAndLaunchEphemeral"
      rgwJobAfterLaunch.taskId shouldBe None
      rgwJobAfterLaunch.slaveId shouldBe None
      rgwJobAfterLaunch.wantingNewOffer shouldBe true

      Thread.sleep(1000)
    }
  }

  it("should update the slave ID and relaunch the task when the persistent offer slave-ID changes") {
    new OneMonitorRunning {
      import module.injector

      val newSlaveReservedOffer = MesosTestHelper.makeBasicOffer(
        slaveId = 409,
        role = "ceph",
        reservationLabels = Some(newLabels(
          Constants.FrameworkIdLabel -> MesosTestHelper.frameworkID.getValue,
          Constants.ReservationIdLabel -> monitorTask.reservationId.map(_.toString).get,
          Constants.JobIdLabel -> monitorTask.id.toString))).
        build

      taskActor ! FrameworkActor.StatusUpdate(
        newTaskStatus(monitorTask.taskId.get, monitorTask.slaveId.get, Protos.TaskState.TASK_KILLED))

      taskActor ! FrameworkActor.ResourceOffers(List(newSlaveReservedOffer))

      inside(gatherResponse(probe, newSlaveReservedOffer, ignoreRevive)) {
        case offerResponse: FrameworkActor.AcceptOffer =>
          offerResponse.operations(0).getType shouldBe Protos.Offer.Operation.Type.LAUNCH
      }

      getTasks.apply(monitorTask.id).slaveId shouldBe (Some(newSlaveReservedOffer.getSlaveId.getValue))
    }
  }
}
