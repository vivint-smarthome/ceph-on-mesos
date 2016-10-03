package org.vivint.ceph

import akka.actor.{ ActorRef, ActorSystem, Props }
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{ Flow, Keep, Sink }
import akka.testkit.ImplicitSender
import com.typesafe.config.{ Config, ConfigFactory, ConfigRenderOptions }
import java.io.File
import java.util.concurrent.atomic.AtomicInteger
import org.apache.commons.io.FileUtils
import org.scalatest.{ BeforeAndAfterAll, FunSpecLike, Matchers }
import org.apache.mesos.Protos
import akka.testkit.TestKit
import org.vivint.ceph.kvstore.KVStore
import org.vivint.ceph.views.ConfigTemplates
import scala.concurrent.{ Await, Awaitable, ExecutionContext }
import scaldi.Module
import scaldi.Injectable._
import scala.concurrent.duration._

class IntegrationTest extends TestKit(ActorSystem("integrationTest"))
    with ImplicitSender with FunSpecLike with Matchers with BeforeAndAfterAll {

  val idx = new AtomicInteger()
  val fileStorePath = new File("tmp/test-store")

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
          clusterNetwork =  "10.11.12.0/24")
      }
      bind [String => String] identifiedBy 'ipResolver to { _: String =>
        "10.11.12.1"
      }
    }
    import module.injector

    val taskActor = inject[ActorRef](classOf[TaskActor])

    inject[FrameworkIdStore].set(
      Protos.FrameworkID.newBuilder.setValue("19c21851-0b06-4f05-bde8-e71450ff2030").build)

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

    val message = receiveOne(5.seconds)
    println(s"I received ${message}")
    Thread.sleep(5000)
  }

}
