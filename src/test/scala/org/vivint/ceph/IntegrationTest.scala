package org.vivint.ceph

import akka.actor.{ ActorRef, ActorSystem, Props }
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{ Flow, Keep, Sink }
import akka.testkit.ImplicitSender
import akka.testkit.TestKit
import com.typesafe.config.{ Config, ConfigFactory, ConfigRenderOptions }
import java.io.File
import java.util.concurrent.atomic.AtomicInteger
import org.apache.commons.io.FileUtils
import org.apache.mesos.Protos
import org.scalatest.{ BeforeAndAfterAll, FunSpecLike, Matchers }
import mesosphere.mesos.protos.Resource.{CPUS, MEM, PORTS, DISK}
import org.vivint.ceph.kvstore.KVStore
import org.vivint.ceph.views.ConfigTemplates
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.concurrent.duration._
import scala.concurrent.{ Await, Awaitable, ExecutionContext }
import scaldi.Injectable._
import scaldi.Module

class IntegrationTest extends TestKit(ActorSystem("integrationTest"))
    with ImplicitSender with FunSpecLike with Matchers with BeforeAndAfterAll {

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

  it("should do things") {
    implicit val ec: ExecutionContext = SameThreadExecutionContext
    val module = new Module {
      bind [ActorRef] identifiedBy(classOf[FrameworkActor]) to self
      bind [ActorRef] identifiedBy(classOf[TaskActor]) to { system.actorOf(Props(new TaskActor)) }
      // bind [KVStore] to { new kvstore.FileStore(new File(fileStorePath, idx.incrementAndGet.toString)) }
      bind [KVStore] to { new kvstore.MemStore }
      bind [OfferOperations] to new OfferOperations
      bind [FrameworkIdStore] to new FrameworkIdStore
      bind [ConfigTemplates] to new ConfigTemplates
      bind [ActorSystem] to system
      bind [AppConfiguration] to {
        AppConfiguration(master = "hai", name = "ceph-test", principal = "ceph", secret = None, role = "ceph",
          zookeeper = "zk://test", offerTimeout = 5.seconds, publicNetwork = "10.11.12.0/24",
          clusterNetwork =  "10.11.12.0/24", storageBackend = "disk")
      }
      bind [String => String] identifiedBy 'ipResolver to { _: String =>
        "10.11.12.1"
      }
    }
    import module.injector

    val taskActor = inject[ActorRef](classOf[TaskActor])

    inject[FrameworkIdStore].set(MesosTestHelper.frameworkID)

    val kvStore = inject[KVStore]
    val configStore = new ConfigStore(inject[KVStore])
    implicit val materializer = ActorMaterializer()

    val cephConfUpdates =
      kvStore.watch("ceph.conf").
        map { _.map { bytes => ConfigFactory.parseString(new String(bytes)) } }.
        collect { case Some(cfg) => cfg }
    val storeConfig = Flow[Config].
      mapAsync(1) { cfg =>
        kvStore.set("ceph.conf", renderConfig(cfg).getBytes)
      }.toMat(Sink.ignore)(Keep.right)


    // Wait for configuration update
    val config = await(cephConfUpdates.runWith(Sink.head))

    // Add a monitor
    await {
      cephConfUpdates.
        take(1).
        map { cfg =>
          ConfigFactory.parseString("deployment.mon.count = 1").withFallback(cfg)
        }.
        runWith(storeConfig)
    }

    receiveOne(5.seconds) shouldBe FrameworkActor.ReviveOffers

    val offer = MesosTestHelper.makeBasicOffer().build

    // Send an offer!
    taskActor ! FrameworkActor.ResourceOffers(List(offer))

    val offerResponse = (receiveOne(5.seconds)).asInstanceOf[FrameworkActor.AcceptOffer]
    offerResponse.offerId shouldBe offer.getId
    val List(reserve, create) = offerResponse.operations
    reserve.hasReserve shouldBe (true)
    create.hasCreate shouldBe (true)

    val reservedOffer = MesosTestHelper.mergeReservation(
      offer, reserve.getReserve, create.getCreate)
    taskActor ! FrameworkActor.ResourceOffers(List(reservedOffer))

    println(receiveOne(5.seconds))
    Thread.sleep(5000)
  }

}
