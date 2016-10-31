package com.vivint.ceph

import akka.actor.{ ActorRef, ActorSystem, Props }
import akka.pattern.BackoffSupervisor
import org.apache.mesos.Protos._
import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.concurrent.Await
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

  bind [() => java.time.ZonedDateTime] identifiedBy 'now to { () => java.time.ZonedDateTime.now() }
  bind [AppConfiguration] to config

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
  bind [ActorSystem] to system destroyWith { _ =>
    try {
      System.err.println("Shutting down actorSystem")
      Await.result(system.terminate(), 10.seconds)
      System.err.println("Actor system shut down")
    } catch {
      case ex: Throwable =>
        System.err.println(s"Unable to shutdown actor system within timeout: ${ex.getMessage}")
        ex.printStackTrace(System.err)
    }
  }

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

  bind [ActorRef] identifiedBy (classOf[ReservationReaperActor]) to {
    system.actorOf(
      BackoffSupervisor.props(childProps = Props(new ReservationReaperActor),
        childName =  "release-actor",
        minBackoff = 1.second,
        maxBackoff = 10.seconds,
        randomFactor = 0.2),
      "release-actor-backoff"
    )
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
  cmdLineOpts.verify()
  val config = AppConfiguration.fromOpts(cmdLineOpts)

  val module = new Universe(config)
  import module.injector
  import scaldi.Injectable._

  implicit val actorSystem = inject[ActorSystem]

  val taskActor = inject[ActorRef](classOf[TaskActor])
  val frameworkActor = inject[ActorRef](classOf[FrameworkActor])
  val httpService = inject[api.HttpService]

  def dieWith(ex: Throwable): Unit = {
    System.err.println(s"Error starting API service: ${ex.getMessage}")
    ex.printStackTrace(System.err)
    try { module.destroy(_ => true) }
    catch { case ex: Throwable => println("le problem") }
    System.exit(1)
  }

  httpService.run().onFailure {
    case  ex: Throwable =>
      dieWith(ex)
  }(ExecutionContext.global)
}
