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

  case class InitialState(
    tasks: Seq[PersistentState],
    frameworkId: Protos.FrameworkID,
    secrets: ClusterSecrets,
    config: CephConfig)
  case class ConfigUpdate(deploymentConfig: Option[CephConfig])
  case class TaskTimer(actions: String, payload: Any)
  case class PersistSuccess(taskId: String, version: Long)
  case class TaskUpdated(previousVersion: PersistentState, nextVersion: PersistentState)

  val log = LoggerFactory.getLogger(getClass)
}

class TaskActor(implicit val injector: Injector) extends Actor with ActorLogging with Stash {
  import TaskActor._
  val kvStore = CrashingKVStore(inject[KVStore])
  val taskStore = TaskStore(kvStore)
  val offerOperations = inject[OfferOperations]
  val frameworkActor = inject[ActorRef](classOf[FrameworkActor])
  implicit val materializer = ActorMaterializer()
  val frameworkIdStore = inject[FrameworkIdStore]
  import ProtoHelpers._

  var frameworkId : Protos.FrameworkID = _
  var _behaviorSet: TaskBehavior = _
  implicit def behaviorSet: TaskBehavior =
    if (_behaviorSet == null)
      throw new IllegalStateException("tried to initialize a behaviorSet before behaviorSet was initializied")
    else
      _behaviorSet

  // var tasks: Map[String, Task] = Map.empty
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

  lib.FutureMonitor.monitor(result, log, "configuration stream")
  lib.FutureMonitor.monitor(kvStore.crashed, log, "kvStore")

  val throttledRevives = Source.queue[Unit](1, OverflowStrategy.dropTail).
    throttle(1, 5.seconds, 1, ThrottleMode.shaping).
    to(Sink.foreach({ _ =>
      frameworkActor ! FrameworkActor.ReviveOffers
    })).
    run

  override def preStart(): Unit = {
    import context.dispatcher

    configStore.storeConfigIfNotExist()
    tasks.addSubscriber {
      case (before, Some(after)) if before.map(_.version).getOrElse(0) != after.version =>
        // version changed?
        import context.dispatcher
        taskStore.save(after.pState).map(_ => PersistSuccess(after.taskId, after.version)) pipeTo self
    }
    tasks.addSubscriber {
      case (Some(before), Some(after)) =>
        tasks.updateTask(
          processEvents(after, List(TaskFSM.TaskUpdated(before))))
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

    logging(kvStore.lock(Constants.LockPath), "acquiring lock").
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
    configStream.cancel()
    throttledRevives.complete()
  }

  final def processEvents(task: Task, events: List[TaskFSM.Event]): Task = events match {
    case event :: rest =>
      log.debug("{} - sending event {}", task.taskId, event.getClass.getName)

      processEvents(
        processDirective(
          task,
          task.behavior.submit(event, task, tasks.all)),
        rest)
    case Nil =>
      task
  }

  final def processHeldEvents(task: Task): Task = {
    task.heldOffer match {
      case Some((offer, resourceMatch)) =>
        processEvents(
          task.copy(heldOffer = None),
          TaskFSM.MatchedOffer(offer, resourceMatch) :: Nil)
      case None =>
        task
    }
  }

  final def processAction(task: Task, action: TaskFSM.Action): Task = {
    log.debug("{} - processing directive response action {}", task.taskId, action.getClass.getName)
    action match {
      case TaskFSM.Hold(offer, resourceMatch) =>
        // Decline existing held offer
        task.heldOffer.foreach {
          case (pending, _) => pending.resultingOperationsPromise.trySuccess(Nil)
        }
        task.copy(heldOffer = Some((offer, resourceMatch)))
      case TaskFSM.Persist(data) =>
        task.copy(persistentState = Some(data))
      case TaskFSM.WantOffers =>
        throttledRevives.offer(())
        task.copy(
          wantingNewOffer = true,
          offerMatcher = offerMatchers.get(task.role))
      case TaskFSM.KillTask =>
        frameworkActor ! FrameworkActor.KillTask(newTaskId(task.taskId))
        task
      case TaskFSM.OfferResponse(pendingOffer, operations) =>
        pendingOffer.resultingOperationsPromise.success(operations.toList)
        task
    }
  }

  final def processDirective(task: Task, directive: TaskFSM.Directive): Task = {
    val taskAfterAction = directive.action.foldLeft(task)(processAction)

    directive.transition match {
      case Some(nextBehaviorFactory) =>
        task.behavior.teardown()
        val nextBehavior = nextBehaviorFactory(task.taskId, context)
        log.info("task {}: Transition {} -> {}", task.taskId, task.behavior.name, nextBehavior.name)
        processHeldEvents(
          initializeBehavior(taskAfterAction.copy(behavior = nextBehavior)))

      case None =>
        taskAfterAction
    }
  }

  final def initializeBehavior(task: Task): Task = {
    log.info("task {}: Initializing behavior {}", task.taskId, task.behavior.name)
    processDirective(task,
      task.behavior.initialize(task, tasks.all))
  }

  def receive = {
    case iState @ InitialState(persistentTaskStates, fId, secrets, _cephConfig) =>
      _behaviorSet = new TaskBehavior(secrets, { () => frameworkId }, { () => cephConfig })
      log.info("InitialState: persistentTaskStates count = {}, fId = {}", persistentTaskStates.length, fId)
        cephConfig = _cephConfig
        frameworkId = fId
      val newTasks = persistentTaskStates.map { p =>
        initializeBehavior(Task.fromState(p))
      }
      newTasks.foreach(tasks.updateTask)

      unstashAll()
      startReconciliation()
    case _ =>
      stash()
  }

  case object ReconcileTimeout

  object DummyCancellable extends Cancellable { def cancel(): Boolean = true; def isCancelled = true }
  var reconciliationTimer: Cancellable = DummyCancellable
  def startReconciliation(): Unit = {
    reconciliationTimer.cancel() // clear out any existing timers
    var taskIdsForReconciliation: Set[String] =
      tasks.values.flatMap { _.taskStatus.map(_.taskId) }(breakOut)
    if (taskIdsForReconciliation.isEmpty) {
      log.info("Skipping reconciliation; no known tasks to reconcile")
      context.become(ready)
      return ()
    }
    log.info("Beginning reconciliation")
    reconciliationTimer = context.system.scheduler.scheduleOnce(30.seconds, self, ReconcileTimeout)(context.dispatcher)
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

  // /** Given an updated task status, persists if persistance has changed (further mondifying the nextVersion).
  //   * Returns the modified version.
  //   *
  //   * TODO - Extract and lock down valid operations. Nobody should be able to update the task state without going
  //   * through this method.
  //   */
  // def updateTask(update: Task): Task = {
  //   val nextTask =
  //     if (tasks.get(update.taskId).flatMap(_.persistentState) != update.persistentState) {
  //       import context.dispatcher
  //       val nextVersion = update.version + 1
  //       taskStore.save(update.pState).map(_ => PersistSuccess(update.taskId, nextVersion)) pipeTo self
  //       update.copy(
  //         version = nextVersion)
  //     } else {
  //       update
  //     }

  //   if (log.isDebugEnabled)
  //   log.debug("task updated: {}", model.PlayJsonFormats.TaskWriter.writes(nextTask))
  //   tasks = tasks.updated(update.taskId, nextTask)
  //   nextTask
  // }

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

        tasks.updateTask(
          processEvents(task, List(TaskFSM.MatchedOffer(pendingOffer, None))))

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
            task.offerMatcher.
              flatMap { _(matchCandidateOffer, task, tasks.values) }.
              map { (_, task) }
          }.
          headOption

        matchingTask match {
          case Some((matchResult, task)) =>
            // TODO - we need to do something with this result
            val pendingOffer = PendingOffer(matchCandidateOffer)
            tasks.updateTask {
              processEvents(
                task.copy(wantingNewOffer = false, offerMatcher = None),
                TaskFSM.MatchedOffer(pendingOffer, Some(matchResult)) :: Nil)}
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

  def ready: Receive = (
    {
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
    }: Receive
  ).
    orElse(default)

  def default: Receive = {
    case cmd: Command =>
      cmd match {
        case GetTasks =>
          sender ! tasks.all

        case UpdateGoal(taskId, goal) =>
          tasks.get(taskId) foreach {
            case task if task.persistentState.isEmpty || task.pState.goal.isEmpty =>
              log.error("Unabled to update run goal for taskId {}; it is not ready", taskId)
            case task =>
              val nextTask = task.copy(persistentState = Some(task.pState.copy(goal = Some(goal))))
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
    case TaskTimer(taskId, payload) =>
      tasks.get(taskId) foreach { task =>
        tasks.updateTask(
          processEvents(task, List(TaskFSM.Timer(payload))))
      }
    case PersistSuccess(taskId, version) =>
      tasks.updatePersistence(taskId, version)
  }

  def applyConfiguration(): Unit = {
    val monTasks = tasks.values.filter ( _.role == TaskRole.Monitor)
    val newMonitorCount = Math.max(0, cephConfig.deployment.mon.count - monTasks.size)
    val newMonitors = Stream.
      continually { Task.forRole(TaskRole.Monitor) }.
      take(newMonitorCount).
      map(initializeBehavior).
      toList

    val cephTasks = tasks.values.filter (_.role == TaskRole.OSD)
    val newOSDCount = Math.max(0, cephConfig.deployment.osd.count - cephTasks.size)
    val newOSDs = Stream.
      continually { Task.forRole(TaskRole.OSD) }.
      take(newOSDCount).
      map(initializeBehavior).
      toList

    log.info("added {} new monitors, {} new OSDs as a result of config update", newMonitors.length, newOSDs.length)

    (newMonitors ++ newOSDs).map(tasks.updateTask)
    offerMatchers = offerMatchFactory(cephConfig)

    var matchersUpdated = false
    tasks.values.foreach { task =>
      if (task.wantingNewOffer) {
        matchersUpdated = true
        tasks.updateTask(
          task.copy(offerMatcher = offerMatchers.get(task.role)))
      }
    }
    if (matchersUpdated) {
      log.info("matchers were updated. Scheduling revive")
      throttledRevives.offer(())
    }
  }
}
