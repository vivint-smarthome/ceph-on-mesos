package org.vivint.ceph

import akka.actor.ActorSystem
import org.apache.curator.framework.{ CuratorFramework, CuratorFrameworkFactory }
import org.apache.mesos.Protos._
import org.vivint.ceph.kvstore.KVStore
import scaldi.Module

trait ZookeeperModule extends Module {
  private val appConfiguration = inject[AppConfiguration]

  import org.apache.curator.retry.ExponentialBackoffRetry
  private val retryPolicy = new ExponentialBackoffRetry(1000, 3)

  bind [CuratorFramework] to CuratorFrameworkFactory.builder.
    connectString(appConfiguration.zookeeper).
    namespace("ceph-on-mesos").
    retryPolicy(retryPolicy).
    build()
}

class Configuration(args: List[String]) extends Module {
  bind [AppConfiguration] to AppConfiguration.fromArgs(args.toList)
}

trait FrameworkModule extends Module {

  bind [FrameworkInfo] to {
    val frameworkId = inject[FrameworkId]
    val options = inject[AppConfiguration]
    val kvStore = inject[KVStore]

    val frameworkBuilder = FrameworkInfo.newBuilder().
      setUser("").
      setName(options.name).
      setCheckpoint(true)

    frameworkId.get.foreach(frameworkBuilder.setId)
    options.principal.foreach(frameworkBuilder.setPrincipal)

    frameworkBuilder.build()
  }
}

class Universe(args: List[String]) extends Configuration(args) with Module with ZookeeperModule with FrameworkModule {
  implicit val system = ActorSystem("ceph-on-mesos")
  bind [KVStore] to (new kvstore.ZookeeperStore(inject[CuratorFramework])(zookeeperDispatcher))
  bind [ActorSystem] to system
  bind [FrameworkId] to new FrameworkId
  val zookeeperDispatcher = system.dispatchers.lookup("zookeeper-dispatcher")
}

object Main extends App {
  val module = new Universe(args.toList)
  import module.injector
  import scaldi.Injectable._

  val options = inject[AppConfiguration]

  // val uri = new File("./test-executor").getCanonicalPath
  // val executor = ExecutorInfo.newBuilder().setExecutorId(ExecutorID.newBuilder().setValue("default"))
  //   .setCommand(CommandInfo.newBuilder().setValue(uri))
  //   .setName(options.name)
  //   .build()

  // val framework = inject[FrameworkInfo]
  // val implicitAcknowledgements = System.getenv("MESOS_EXPLICIT_ACKNOWLEDGEMENTS") == null
  // val scheduler = new TestScheduler(implicitAcknowledgements, executor, 5)
  // val credentials = for {
  //   principal <- options.principal
  //   secret <- options.secret
  // } yield Credential.newBuilder().
  //   setPrincipal(principal).
  //   setSecret(secret).
  //   build()

  // val driver = credentials match {
  //   case Some(c) =>
  //     new MesosSchedulerDriver(scheduler, framework, options.master, implicitAcknowledgements, c)
  //   case None =>
  //     new MesosSchedulerDriver(scheduler, framework, options.master, implicitAcknowledgements)
  // }

  // val status = if (driver.run() == Status.DRIVER_STOPPED) 0 else 1
  // driver.stop()
  // Thread.sleep(500)
  // System.exit(status)

  // println("hai")
}
