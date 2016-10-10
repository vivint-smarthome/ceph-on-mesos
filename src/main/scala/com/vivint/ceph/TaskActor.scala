package com.vivint.ceph

import akka.actor.{ Actor, ActorLogging, ActorRef, Cancellable, Stash }
import akka.pattern.pipe
import akka.stream.{ ActorMaterializer, OverflowStrategy, ThrottleMode }
import akka.stream.scaladsl.{ Flow, Keep, Sink, Source }
import lib.FutureHelpers.tSequence
import mesosphere.mesos.matcher._
import org.apache.mesos.Protos
import org.slf4j.LoggerFactory
import com.vivint.ceph.kvstore.{KVStore, CrashingKVStore}
import com.vivint.ceph.model._
import scala.collection.breakOut
import scala.collection.immutable.{Iterable, Seq}
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.{Success, Failure}
import scaldi.Injectable._
import scaldi.Injector

object TaskActor {
  sealed trait Command
  case object GetTasks extends Command
  case class UpdateGoal(taskId: String, goal: RunState.EnumVal) extends Command
  case class TaskTimer(taskId: String, timerName: String)

  val log = LoggerFactory.getLogger(getClass)
}

class TaskActor(implicit val injector: Injector) extends Actor with ActorLogging with Stash {
  import TaskActor._
  case class ConfigUpdate(deploymentConfig: Option[CephConfig])
  case class PersistSuccess(taskId: String, version: Long)
  case class InitialState(
    tasks: Seq[PersistentState],
    frameworkId: Protos.FrameworkID,
    secrets: ClusterSecrets,
    config: CephConfig)

  val kvStore = CrashingKVStore(inject[KVStore])
  val taskStore = TaskStore(kvStore)
  val offerOperations = inject[OfferOperations]
  val frameworkActor = inject[ActorRef](classOf[FrameworkActor])
  implicit val materializer = ActorMaterializer()
  val frameworkIdStore = inject[FrameworkIdStore]
  import ProtoHelpers._

  var frameworkId : Protos.FrameworkID = _
  var _taskFSM: TaskFSM = _
  implicit def taskFSM: TaskFSM =
    if (_taskFSM == null)
      throw new IllegalStateException("tried to initialize a behavior before taskFSM was initializied")
    else
      _taskFSM

  val tasks = new TasksState(log)

  var cephConfig: CephConfig = _

  val config = inject[AppConfiguration]
  val configStore = ConfigStore(kvStore)
  val offerMatchFactory = new MasterOfferMatchFactory
  var offerMatchers: Map[TaskRole.EnumVal, OfferMatchFactory.OfferMatcher] = Map.empty

  val getFirstConfigUpdate =
    Flow[Option[CephConfig]].
      collect { case Some(cfg) => cfg }.
      toMat(Sink.head)(Keep.right)

  val ((configStream, deployConfigF), result) = configStore.stream.
    dropWhile(_.isEmpty). // Handles initial bootstrap
    alsoToMat(getFirstConfigUpdate)(Keep.both).
    map(ConfigUpdate).
    toMat(Sink.foreach(self ! _))(Keep.both).
    run

  val throttledRevives = Source.queue[Unit](1, OverflowStrategy.dropTail).
    throttle(1, 5.seconds, 1, ThrottleMode.shaping).
    to(Sink.foreach({ _ =>
      frameworkActor ! FrameworkActor.ReviveOffers
    })).
    run

  val orchestrator = new Orchestrator(tasks)
  val lock = kvStore.lock(Constants.LockPath)

  override def preStart(): Unit = {
    import context.dispatcher

    lib.FutureMonitor.monitor(result, log, "configuration stream")
    lib.FutureMonitor.monitor(kvStore.crashed, log, "kvStore")

    configStore.storeConfigIfNotExist()
    tasks.addSubscriber {
      case (before, Some(after)) if before.map(_.version).getOrElse(0) != after.version =>
        // version changed?
        import context.dispatcher
        taskStore.save(after.pState).map(_ => PersistSuccess(after.taskId, after.version)) pipeTo self
    }

    log.info("pulling initial state for TaskActor")
    def logging[T](f: Future[T], desc: String): Future[T] = {
      log.debug(s"${desc} : pulling state")
      f.onComplete {
        case Success(_) => log.debug("{} : success", desc)
        case Failure(ex) =>
          log.error(ex, "{}: failure", desc)
      }
      f
    }

    logging(lock, "acquiring lock").
      flatMap { _ =>
        tSequence(
          logging(taskStore.getTasks, "taskStore.getTasks"),
          logging(frameworkIdStore.get, "frameworkIdStore.get"),
          logging(ClusterSecretStore.createOrGenerateSecrets(kvStore), "secrets"),
          logging(deployConfigF, "deploy config"))
      }.
      map(InitialState.tupled).
      pipeTo(self)
  }

  override def postStop(): Unit = {
    // try and release the lock
    lock.foreach { _.cancel() }(context.dispatcher)

    configStream.cancel()
    throttledRevives.complete()
  }

  def receive = {
    case iState @ InitialState(persistentTaskStates, fId, secrets, _cephConfig) =>
      val behaviorSet = new TaskBehavior(secrets, { () => frameworkId }, { () => cephConfig })
      _taskFSM = new TaskFSM(tasks,
        log = log,
        behaviorSet = behaviorSet,
        setTimer = { (taskId, timerName, duration) =>
          import context.dispatcher
          context.system.scheduler.scheduleOnce(duration) {
            self ! TaskTimer(taskId, timerName)
          }},
        revive = { () => throttledRevives.offer(()) },
        killTask = { (taskId) =>
          frameworkActor ! FrameworkActor.KillTask(newTaskId(taskId))
        }
      )

      log.info("InitialState: persistentTaskStates count = {}, fId = {}", persistentTaskStates.length, fId)
        cephConfig = _cephConfig
        frameworkId = fId
      val newTasks = persistentTaskStates.map { p =>
        taskFSM.initializeBehavior(
          Task.fromState(p, defaultBehavior = taskFSM.defaultBehavior))
      }
      newTasks.foreach(tasks.updateTask)

      unstashAll()
      startReconciliation()
    case _ =>
      stash()
  }


  def startReconciliation(): Unit = {
    case object ReconcileTimeout

    var taskIdsForReconciliation: Set[String] =
      tasks.values.flatMap { _.taskStatus.map(_.taskId) }(breakOut)

    if (taskIdsForReconciliation.isEmpty) {
      log.info("Skipping reconciliation; no known tasks to reconcile")
      context.become(ready)
      return ()
    }

    log.info("Beginning reconciliation")
    val reconciliationTimer = context.system.scheduler.scheduleOnce(30.seconds, self, ReconcileTimeout)(context.dispatcher)
    var reconciledResult = List.empty[(Task, Protos.TaskStatus)]
    frameworkActor ! FrameworkActor.Reconcile(tasks.values.flatMap(_.taskStatus).map(_.toMesos)(breakOut))
    context.become {
      case ReconcileTimeout =>
        throw new Exception("timeout during reconciliation")
      case FrameworkActor.ResourceOffers(offers) =>
        offers.foreach { o =>
          frameworkActor ! FrameworkActor.DeclineOffer(o.getId, Some(5.seconds))
        }
      case FrameworkActor.StatusUpdate(taskStatus) =>
        val taskId = taskStatus.getTaskId.getValue
        tasks.get(taskId) match {
          case Some(task) =>
            if (log.isDebugEnabled)
              log.debug("received stats update {}", taskStatus)
            else
              log.info("received status update for {}", taskId)
            taskIdsForReconciliation -= taskId
            reconciledResult = (task, taskStatus) :: reconciledResult

          case None =>
            log.info("received status update for unknown task {}; going to try and kill it", taskId)
            // The task is ours but we don't recognize it. Kill it.
            frameworkActor ! FrameworkActor.KillTask(taskStatus.getTaskId)
        }

        if (taskIdsForReconciliation.isEmpty) {
          reconciledResult.foreach { case (task, taskStatus) =>
            tasks.updateTask(task.copy(taskStatus = Some(TaskStatus.fromMesos(taskStatus))))
          }

          reconciliationTimer.cancel()
          unstashAll()
          log.info("reconciliation complete")
          context.become(ready)
        }
      case _ => stash()
    }
  }

  object OurFrameworkId {
    def unapply(fId: Protos.FrameworkID): Boolean = {
      fId == frameworkId
    }
    def unapply(fId: String) : Boolean = {
      fId == frameworkId.getValue
    }
  }

  /** Looking at reservation labels, routes the offer to the appropriate
    *
    */
  def handleOffer(offer: Protos.Offer): Future[Iterable[Protos.Offer.Operation]] = {

    val reservedGroupings = offer.resources.groupBy { r =>
      r.reservation.
        flatMap(_.labels).
        map { labels =>
          (labels.get(Constants.TaskIdLabel), labels.get(Constants.FrameworkIdLabel))
        }.
        getOrElse {
          (None, None)
        }
    }

    /* TODO - we could end up issuing the same set of resources twice for the same task in the case that a task is
    trying to grow in resources. That's not a supported use case right now. At the point it is supported, we can do
    mapping / grouping */
    val operations = reservedGroupings.map {
      case ((Some(taskId), Some(OurFrameworkId())), resources)
          if tasks.get(taskId).flatMap(_.slaveId).contains(offer.getSlaveId.getValue) =>
        val task = tasks(taskId)
        val pendingOffer = PendingOffer(offer.withResources(resources))

        // TODO - move updateTask to taskFSM
        taskFSM.handleEvent(task, TaskFSM.MatchedOffer(pendingOffer, None))

        pendingOffer.resultingOperations

      case ((Some(_), Some(OurFrameworkId())), resources) =>
        Future.successful(offerOperations.unreserveOffer(resources))
      case ((None, None), resources) =>
        val matchCandidateOffer = offer.withResources(resources)
        val matchingTask = tasks.values.
          toStream.
          filter(_.readyForOffer).
          flatMap { task =>
            val selector = ResourceMatcher.ResourceSelector.any(Set("*", config.role))
            offerMatchers.get(task.role).
              flatMap { _(matchCandidateOffer, task, tasks.values) }.
              map { (_, task) }
          }.
          headOption

        matchingTask match {
          case Some((matchResult, task)) =>
            // TODO - we need to do something with this result
            val pendingOffer = PendingOffer(matchCandidateOffer)
            taskFSM.handleEvent(
              task.copy(wantingNewOffer = false),
              TaskFSM.MatchedOffer(pendingOffer, Some(matchResult)))

            pendingOffer.resultingOperations
          case _ =>
            Future.successful(Nil)
        }
      case ((_, _), _) =>
        Future.successful(Nil)
    }
    import context.dispatcher
    Future.sequence(operations).map(_.flatten)
  }

  def ready: Receive = {
    case FrameworkActor.StatusUpdate(taskStatus) if tasks contains taskStatus.getTaskId.getValue =>
        val priorState = tasks(taskStatus.getTaskId.getValue)
        val nextState = priorState.copy(taskStatus = Some(TaskStatus.fromMesos(taskStatus)))
        tasks.updateTask(nextState)

      case FrameworkActor.ResourceOffers(offers) =>
        offers.foreach { offer =>
          log.debug("received offer\n{}", offer)
          import context.dispatcher
          handleOffer(offer).
            map { ops =>
              log.debug("response for offer {}: {}", offer.getId.getValue, ops.map(_.getType.getValueDescriptor))
              if (ops.isEmpty)
                FrameworkActor.DeclineOffer(offer.getId, Some(2.minutes))
              else
                FrameworkActor.AcceptOffer(offer.getId, ops.toList)
            }.
            pipeTo(frameworkActor)
        }

    case cmd: Command =>
      cmd match {
        case GetTasks =>
          sender ! tasks.all

        case UpdateGoal(taskId, goal) =>
          tasks.get(taskId) foreach {
            case task if task.pState.goal.isEmpty =>
              log.error("Unabled to update run goal for taskId {}; it is not ready", taskId)
            case task =>
              val nextTask = task.withGoal(Some(goal))
              tasks.updateTask(nextTask)
          }
      }

    case FrameworkActor.Connected =>
      startReconciliation()

    case ConfigUpdate(Some(newCfg)) =>
      cephConfig = newCfg
      applyConfiguration()
    case ConfigUpdate(None) =>
      log.warning("Ceph config went missing / unparseable. Changes not applied")
    case TaskTimer(taskId, timerName) =>
      taskFSM.onTimer(taskId, timerName)
    case PersistSuccess(taskId, version) =>
      tasks.updatePersistence(taskId, version)
  }

  def applyConfiguration(): Unit = {
    val newTasks = List(
      TaskRole.Monitor -> cephConfig.deployment.mon.count,
      TaskRole.RGW -> cephConfig.deployment.rgw.count,
      TaskRole.OSD -> cephConfig.deployment.osd.count).
      flatMap {
        case (role, size) =>
          val roleTasks = tasks.values.filter( _.role == role)
          val newCount = Math.max(0, size - roleTasks.size)
          Stream.
            continually { Task.forRole(role, taskFSM.defaultBehavior) }.
            take(newCount).
            map(taskFSM.initializeBehavior)
      }.toList

    if (log.isInfoEnabled) {
      val newDesc = newTasks.groupBy(_.role).map { case (r, vs) => s"${r} -> ${vs.length}" }.mkString(", ")
      log.info("added {} as a result of config update", newDesc)
    }

    newTasks.foreach(tasks.updateTask)
    offerMatchers = offerMatchFactory(cephConfig)

    if (tasks.values.exists(_.wantingNewOffer)) {
      log.info("matchers were updated. Scheduling revive")
      throttledRevives.offer(())
    }
  }
}
