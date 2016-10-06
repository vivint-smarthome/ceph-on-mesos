package com.vivint.ceph

import akka.actor.{ Actor, ActorContext, ActorLogging, ActorRef, Cancellable, DeadLetter, Stash }
import akka.pattern.pipe
import java.util.Collections
import java.util.concurrent.TimeoutException
import org.apache.mesos.Protos._
import org.apache.mesos._
import org.slf4j.LoggerFactory
import com.vivint.ceph.kvstore.{KVStore, CrashingKVStore}
import scala.concurrent.Await
import scaldi.Injectable._
import scaldi.Injector
import scala.concurrent.duration._
import scala.collection.mutable
import scala.collection.immutable.Iterable
import scala.collection.JavaConverters._
import FrameworkActor._

class FrameworkActor(implicit val injector: Injector) extends Actor with ActorLogging with Stash {
  val kvStore = CrashingKVStore(inject[KVStore])
  val frameworkStore = inject[FrameworkIdStore]
  val frameworkTemplate = inject[FrameworkInfo]
  val credentials = inject[Option[Credential]]
  val options = inject[AppConfiguration]
  val pendingOffers = mutable.Map.empty[OfferID, Cancellable]
  lazy val taskActor = inject[ActorRef](classOf[TaskActor])

  case class FrameworkIdLoaded(o: Option[FrameworkID])
  override def preStart(): Unit = {
    import context.dispatcher
    frameworkStore.initial.map(FrameworkIdLoaded) pipeTo self
  }

  override def preRestart(reason: Throwable, message: Option[Any]): Unit = {
    // crash hard
    context.stop(self)
    log.error(reason, s"Exiting due to Framework Actor crash. Last message = {}", message)
    Thread.sleep(100)
    System.exit(1)
  }

  var frameworkId: Option[FrameworkID] = None

  def receive = {
    case FrameworkIdLoaded(optFrameworkId) =>
      frameworkId = optFrameworkId
      log.info("frameworkId state read; optFrameworkId = {}", optFrameworkId)

      unstashAll()
      connect()
    case _ =>
      stash()
  }

  // def waitForFirstRegistration(timer: Cancellable): Receive =

  def connect(): Unit = {
    val framework = frameworkId.map { id =>
      frameworkTemplate.toBuilder().setId(id).build
    } getOrElse {
      frameworkTemplate
    }

    log.info("starting scheduler")

    val scheduler = new FrameworkActorScheduler

    val driver = credentials match {
      case Some(c) =>
        new MesosSchedulerDriver(scheduler, framework, options.master, true, c)
      case None =>
        new MesosSchedulerDriver(scheduler, framework, options.master, true)
    }
    // We exit on exception in this actor so we don't have to worry about closing the driver
    val status = driver.start()
    if (status != Status.DRIVER_RUNNING)
      throw new RuntimeException(s"Error starting framework: ${status}")
    // status.getNumber == Stat

    import context.dispatcher
    val timeout = context.system.scheduler.scheduleOnce(30.seconds, self, 'timeout)
    context.become({
      case 'timeout =>
        throw new TimeoutException("timed out while attempting to register framework with mesos")
      case r: Registered =>
        log.info("Registered! ID = " + r.frameworkId.getValue)
        if (frameworkId.isEmpty) {
          // It's pretty crucial that we don't continue if this fails
          frameworkId = Some(r.frameworkId)
          Await.result(frameworkStore.set(r.frameworkId), 30.seconds)
        }

        timeout.cancel()
        stash() // we want to re-handle this message in the next behavior
        unstashAll()
        context.become(registationHandler)
      case _ =>
        stash()
    })
  }

  context.system.eventStream.subscribe(self, classOf[DeadLetter])

  val registationHandler: Receive = {
    case Registered(driver, newFrameworkId, masterInfo) =>
      taskActor ! Connected
      unstashAll()
      context.become(connected(driver))

    case Reregistered(driver, masterInfo) =>
      log.info("Reregistered")
      taskActor ! Connected
      unstashAll()
      context.become(connected(driver))
  }

  def disconnected: Receive = registationHandler orElse {
    case Error(er) =>
      throw new RuntimeException("Framework error: s{er}")
    case _ =>
      stash()
  }

  def connected(driver: SchedulerDriver): Receive = registationHandler orElse {
    case newDriver: SchedulerDriver =>
      context.become(connected(driver))

    case Disconnected =>
      pendingOffers.clear()
      context.become(disconnected)

    case statusUpdate: StatusUpdate =>
      taskActor ! statusUpdate

    case o @ ResourceOffers(offers) =>
      log.debug("received {} offers from mesos. Forwarding to TaskActor", offers.length)
      offers.foreach { offer =>
        pendingOffers(offer.getId) = context.system.scheduler.scheduleOnce(options.offerTimeout) {
          log.debug(s"Timing out offer {}", offer.getId)
          self ! DeclineOffer(offer.getId, Some(0.seconds))
        }(context.dispatcher)
      }
      taskActor ! o

    case d: DeadLetter =>
      d match {
        case DeadLetter(resourceOffers: ResourceOffers, self, taskActor) =>
          log.info("offer was not received. Declining")
          resourceOffers.offers.foreach { offer =>
            self ! DeclineOffer(offer.getId, Some(30.seconds))
          }
        case _ =>
      }

    case cmd: Command =>
      cmd match {
        case ReviveOffers =>
          driver.reviveOffers()

        /* commands */
        case DeclineOffer(offerId, refuseFor) =>
          processingOffer(offerId) {
            log.debug(s"Decline offer {}", offerId)
            driver.declineOffer(
              offerId,
              ProtoHelpers.newFilters(refuseFor))
          }
        case AcceptOffer(offerId, operations, refuseFor) =>
          processingOffer(offerId) {
            if(log.isDebugEnabled)
              log.debug(s"Operations on ${offerId.getValue}:\n${operations.mkString("\n")}")
            driver.acceptOffers(
              Collections.singleton(offerId),
              operations.asJavaCollection,
              ProtoHelpers.newFilters(refuseFor.orElse(Some(0.seconds))))
          }

        case Reconcile(tasks) =>
          val status = driver.reconcileTasks(tasks.asJava)
          log.info("beginning reconciliation; status is {}", status)
        case KillTask(taskId) =>
          driver.killTask(taskId)
      }
  }


  def processingOffer(offerId: OfferID)(body: => Unit): Unit = {
    pendingOffers.get(offerId).map { timer =>
      body
      timer.cancel()
      pendingOffers.remove(offerId)
      true
    }
  }
}

object FrameworkActor {
  sealed trait ConnectEvent
  /* Mesos updates */
  case class Registered(driver: SchedulerDriver, frameworkId: FrameworkID, masterInfo: MasterInfo) extends ConnectEvent
  case class Reregistered(driver: SchedulerDriver, masterInfo: MasterInfo) extends ConnectEvent
  case object Connected
  case object Disconnected
  case class ResourceOffers(offers: List[Offer])
  case class OfferRescinded(offerId: OfferID)
  case class StatusUpdate(status: TaskStatus)
  case class FrameworkMessage(executorId: ExecutorID, slaveId: SlaveID, data: Array[Byte])
  case class SlaveLost(slaveId: SlaveID)
  case class ExecutorLost(executorId: ExecutorID, slaveId: SlaveID, status: Int)
  case class Error(message: String)

  /* Mesos commands */
  sealed trait Command
  sealed trait OfferResponseCommand extends Command {
    def offerId: OfferID
  }

  case class DeclineOffer(offerId: OfferID, refuseFor: Option[FiniteDuration] = None) extends OfferResponseCommand
  case class AcceptOffer(offerId: OfferID, operations: Seq[Offer.Operation] = Nil,
    refuseFor: Option[FiniteDuration] = None) extends OfferResponseCommand
  case class Reconcile(tasks: List[TaskStatus]) extends Command
  case class KillTask(taskId: TaskID) extends Command
  case object ReviveOffers extends Command
}


class FrameworkActorScheduler(implicit context: ActorContext)
    extends Scheduler {
  val log = LoggerFactory.getLogger(getClass)
  import FrameworkActor._
  override def registered(driver: SchedulerDriver, frameworkId: FrameworkID, masterInfo: MasterInfo): Unit = {
    log.info("framework registered; frameworkId = {}, masterInfo = {}", frameworkId: Any, masterInfo : Any)
    context.self ! Registered(driver, frameworkId, masterInfo)
    log.info("context.self = {}", context.self)
  }

  override def reregistered(driver: SchedulerDriver, masterInfo: MasterInfo): Unit = {
    context.self ! Reregistered(driver, masterInfo)
  }

  override def disconnected(driver: SchedulerDriver): Unit = {
    context.self ! Disconnected
  }

  override def resourceOffers(driver: SchedulerDriver, offers: java.util.List[Offer]): Unit = {
    import scala.collection.JavaConversions._
    context.self ! driver
    context.self ! ResourceOffers(offers.toList)
  }

  override def offerRescinded(driver: SchedulerDriver, offerId: OfferID): Unit = {
    context.self ! driver
    context.self ! OfferRescinded(offerId)
  }

  override def statusUpdate(driver: SchedulerDriver, status: TaskStatus): Unit = {
    context.self ! driver
    context.self ! StatusUpdate(status)
  }

  override def frameworkMessage(driver: SchedulerDriver, executorId: ExecutorID, slaveId: SlaveID, data: Array[Byte]):
      Unit = {
    context.self ! driver
    context.self ! FrameworkMessage(executorId, slaveId, data)
  }

  override def slaveLost(driver: SchedulerDriver, slaveId: SlaveID): Unit = {
    context.self ! driver
    context.self ! SlaveLost(slaveId)
  }

  override def executorLost(driver: SchedulerDriver, executorId: ExecutorID, slaveId: SlaveID, status: Int): Unit = {
    context.self ! driver
    context.self ! ExecutorLost(executorId, slaveId, status)
  }

  def error(driver: SchedulerDriver, message: String): Unit = {
    log.error(s"Error: {}", message)
    context.self ! Error(message)
  }
}
