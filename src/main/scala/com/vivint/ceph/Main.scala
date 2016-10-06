package com.vivint.ceph

import akka.actor.{ ActorRef, ActorSystem, Props }
import akka.pattern.BackoffSupervisor
import java.net.InetAddress
import org.apache.mesos.Protos._
import scala.concurrent.duration._
import com.vivint.ceph.kvstore.KVStore
import scaldi.Module

trait FrameworkModule extends Module {

  bind [FrameworkInfo] to {
    val config = inject[AppConfiguration]
    val kvStore = inject[KVStore]

    val frameworkBuilder = FrameworkInfo.newBuilder().
      setUser("").
      setName(config.name).
      setCheckpoint(true).
      setRole(config.role).
      setPrincipal(config.principal).
      setCheckpoint(true).
      setFailoverTimeout(config.failoverTimeout.toDouble)

    frameworkBuilder.build()
  }
}

class Universe(config: AppConfiguration) extends FrameworkModule with Module {
  implicit val system = ActorSystem("ceph-on-mesos")

  bind [AppConfiguration] to config

  bind [String => String] identifiedBy 'ipResolver to { InetAddress.
    getByName(_: String).
    getHostAddress
  }

  bind [KVStore] to {
    config.storageBackend match {
      case "zookeeper" =>
        new kvstore.ZookeeperStore
      case "file" =>
        new kvstore.FileStore(new java.io.File("data"))
      case "memory" =>
        new kvstore.MemStore
    }
  }

  bind [FrameworkIdStore] to (new FrameworkIdStore)
  bind [ActorSystem] to system
  bind [views.ConfigTemplates] to new views.ConfigTemplates
  bind [OfferOperations] to new OfferOperations
  bind [Option[Credential]] to {
    config.secret.map { secret =>
      Credential.newBuilder().
        setPrincipal(config.principal).
        setSecret(secret).
        build()
    }
  }

  bind [ActorRef] identifiedBy (classOf[TaskActor]) to {
    system.actorOf(
      BackoffSupervisor.props(childProps = Props(new TaskActor),
        childName =  "task-actor",
        minBackoff = 1.second,
        maxBackoff = 10.seconds,
        randomFactor = 0.2),
      "task-actor-backoff"
    )
  }

  bind [ActorRef] identifiedBy (classOf[FrameworkActor]) to {
    system.actorOf(Props(new FrameworkActor), "framework-actor")
  }
  bind [api.HttpService] to { new api.HttpService }
}

object Main extends App {
  val cmdLineOpts = new CephFrameworkOptions(args.toList)
  val config = AppConfiguration.fromOpts(cmdLineOpts)

  val module = new Universe(config)
  import module.injector
  import scaldi.Injectable._

  implicit val actorSystem = inject[ActorSystem]

  val taskActor = inject[ActorRef](classOf[TaskActor])
  val frameworkActor = inject[ActorRef](classOf[FrameworkActor])
  val httpService = inject[api.HttpService]
  httpService.run()
}
