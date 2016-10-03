package org.vivint.ceph

import akka.actor.{ ActorRef, ActorSystem, Props }
import java.net.InetAddress
import org.apache.curator.framework.{ CuratorFramework, CuratorFrameworkFactory }
import org.apache.mesos.MesosSchedulerDriver
import org.apache.mesos.Protos._
import org.vivint.ceph.kvstore.KVStore
import scala.concurrent.Future
import scaldi.Module

trait ZookeeperModule extends Module {
  private val appConfiguration = inject[AppConfiguration]


}

class Configuration(args: List[String]) extends Module {
  bind [AppConfiguration] to AppConfiguration.fromArgs(args.toList)
}

trait FrameworkModule extends Module {

  bind [FrameworkInfo] to {
    val options = inject[AppConfiguration]
    val kvStore = inject[KVStore]

    val frameworkBuilder = FrameworkInfo.newBuilder().
      setUser("").
      setName(options.name).
      setCheckpoint(true).
      setPrincipal(options.principal)

    frameworkBuilder.build()
  }
}

class Universe(args: List[String]) extends Configuration(args) with Module with ZookeeperModule /*with FrameworkModule*/ {
  implicit val system = ActorSystem("ceph-on-mesos")
  bind [ActorRef] identifiedBy (classOf[kvstore.ZookeeperActor]) to {
    system.actorOf(
      Props(new kvstore.ZookeeperActor).withDispatcher("zookeeper-dispatcher"),
      "zookeeper-actor")
  }

  bind [String => String] identifiedBy 'ipResolver to { InetAddress.
    getByName(_: String).
    getHostAddress
  }

  bind [KVStore] to (new kvstore.ZookeeperStore)
  bind [FrameworkIdStore] to (new FrameworkIdStore)
  bind [ActorSystem] to system
  bind [views.ConfigTemplates] to new views.ConfigTemplates
  bind [OfferOperations] to new OfferOperations
  bind [Option[Credential]] to {
    val options = inject[AppConfiguration]
    options.secret.map { secret =>
      Credential.newBuilder().
        setPrincipal(options.principal).
        setSecret(secret).
        build()
    }
  }

  bind [ActorRef] identifiedBy (classOf[TaskActor]) to {
    system.actorOf(Props(new TaskActor), "framework-actor")
  }

  bind [ActorRef] identifiedBy (classOf[FrameworkActor]) to {
    system.actorOf(Props(new FrameworkActor), "framework-actor")
  }
}

object Main extends App {
  val module = new Universe(args.toList)
  import module.injector
  import scaldi.Injectable._

  implicit val actorSystem = inject[ActorSystem]

  val taskActor = inject[ActorRef](classOf[TaskActor])
  val frameworkActor = inject[ActorRef](classOf[FrameworkActor])
}
