package org.vivint.ceph

import akka.actor.{ Actor, ActorContext, ActorLogging, ActorRef, Cancellable, Stash }
import akka.pattern.pipe
import java.util.Collections
import org.apache.mesos.Protos._
import org.apache.mesos._
import org.slf4j.LoggerFactory
import org.vivint.ceph.kvstore.{KVStore, CrashingKVStore}
import scala.concurrent.Await
import scaldi.Injectable._
import scaldi.Injector
import scala.concurrent.duration._
import scala.collection.mutable
import scala.collection.JavaConverters._

class FrameworkActor(implicit val injector: Injector) extends Actor with ActorLogging with Stash {
  val kvStore = CrashingKVStore(inject[KVStore])
  val frameworkStore = FrameworkIdStore(kvStore)
  val frameworkTemplate = inject[FrameworkInfo]
  val credentials = inject[Option[Credential]]
  val options = inject[AppConfiguration]
  val pendingOffers = mutable.Map.empty[OfferID, Cancellable]
  lazy val taskActor = inject[ActorRef](classOf[TaskActor])

  case class FrameworkIdLoaded(o: Option[FrameworkID])
  override def preStart(): Unit = {
    import context.dispatcher
    frameworkStore.get.map(FrameworkIdLoaded) pipeTo self
  }

  override def preRestart(reason: Throwable, message: Option[Any]): Unit = {
    // crash hard
    log.error(reason, s"Exiting due to Framework Actor crash. Last message = {}", message)
    System.exit(1)
  }

  var initialFrameworkId: Option[FrameworkID] = None

  def receive = {
    case FrameworkIdLoaded(optFrameworkId) =>
      initialFrameworkId = optFrameworkId
      log.info("zookeeper connection established and frameworkId state read")
      val framework = optFrameworkId.map { id =>
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
      driver.run()
      unstashAll()
      context.become(disconnected)
    case _ =>
      stash()
  }

  import FrameworkActor._

  val registationHandler: Receive = {
    case Registered(driver, frameworkId, masterInfo) =>
      if (initialFrameworkId.isEmpty) {
        initialFrameworkId = Some(frameworkId)
        // It's pretty crucial that we don't continue if this fails
        Await.result(frameworkStore.set(frameworkId), 30.seconds)
      }

      log.info("Registered! ID = " + frameworkId.getValue)
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
      offers.foreach { offer =>
        pendingOffers(offer.getId) = context.system.scheduler.scheduleOnce(options.offerTimeout) {
          log.debug(s"Timing out offer {}", offer.getId)
          self ! DeclineOffer(offer.getId, Some(0.seconds))
        }(context.dispatcher)
      }
      taskActor ! o

    case cmd: Command =>
      cmd match {
        /* commands */
        case DeclineOffer(offerId, refuseFor) =>
          processingOffer(offerId) {
            log.debug(s"Decline offer {}", offerId)
            driver.declineOffer(offerId, ProtoHelpers.filters(refuseFor))
          }
        case AcceptOffer(offerId, operations) =>
          processingOffer(offerId) {
            if(log.isDebugEnabled)
              log.debug(s"Operations on $offerId:\n${operations.mkString("\n")}")
            driver.acceptOffers(
              Collections.singleton(offerId),
              operations.asJava,
              ProtoHelpers.filters(Some(0.seconds)))
          }

        case Reconcile(tasks) =>
          val status = driver.reconcileTasks(tasks.asJava)
          log.info("beginning reconciliation; status is {}", status)
      }


    case catchAll =>
      println(s"Received ${catchAll}")
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
  case class DeclineOffer(offerId: OfferID, refuseFor: Option[FiniteDuration] = None) extends Command
  case class AcceptOffer(offerId: OfferID, operations: List[Offer.Operation] = Nil) extends Command
  case class Reconcile(tasks: List[TaskStatus]) extends Command
}


class FrameworkActorScheduler(implicit context: ActorContext)
    extends Scheduler {
  val log = LoggerFactory.getLogger(getClass)
  import FrameworkActor._
  override def registered(driver: SchedulerDriver, frameworkId: FrameworkID, masterInfo: MasterInfo): Unit = {
    context.self ! Registered(driver, frameworkId, masterInfo)
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
    log.error(s"Error in frameworkActor {}", message)
    context.self ! Error(message)
  }
}
