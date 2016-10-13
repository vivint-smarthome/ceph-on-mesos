package com.vivint.ceph

import akka.actor.{ Actor, ActorLogging, ActorRef, Cancellable, Stash }
import akka.pattern.pipe
import akka.stream.{ ActorMaterializer, OverflowStrategy, ThrottleMode }
import akka.stream.scaladsl.{ Flow, Keep, Sink, Source }
import java.util.UUID
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
  case object GetJobs extends Command
  case class UpdateGoal(id: UUID, goal: RunState.EnumVal) extends Command
  case class JobTimer(id: UUID, timerName: String)

  val log = LoggerFactory.getLogger(getClass)
}

class TaskActor(implicit val injector: Injector) extends Actor with ActorLogging with Stash {
  import TaskActor._
  case class ConfigUpdate(deploymentConfig: Option[CephConfig])
  case class PersistSuccess(id: UUID, version: Long)
  case class InitialState(
    tasks: Seq[PersistentState],
    frameworkId: Protos.FrameworkID,
    secrets: ClusterSecrets,
    config: CephConfig)

  val kvStore = CrashingKVStore(inject[KVStore])
  val taskStore = JobStore(kvStore)
  val offerOperations = inject[OfferOperations]
  val frameworkActor = inject[ActorRef](classOf[FrameworkActor])
  implicit val materializer = ActorMaterializer()
  val frameworkIdStore = inject[FrameworkIdStore]
  import ProtoHelpers._

  var frameworkId : Protos.FrameworkID = _
  var _taskFSM: JobFSM = _
  implicit def taskFSM: JobFSM =
    if (_taskFSM == null)
      throw new IllegalStateException("tried to initialize a behavior before taskFSM was initializied")
    else
      _taskFSM

  val jobs = new JobsState(log)

  var cephConfig: CephConfig = _

  val config = inject[AppConfiguration]
  val configStore = ConfigStore(kvStore)
  val offerMatchFactory = new MasterOfferMatchFactory
  var offerMatchers: Map[JobRole.EnumVal, OfferMatchFactory.OfferMatcher] = Map.empty

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

  val orchestrator = new Orchestrator(jobs)
  val lock = kvStore.lock(Constants.LockPath)

  override def preStart(): Unit = {
    import context.dispatcher

    lib.FutureMonitor.monitor(result, log, "configuration stream")
    lib.FutureMonitor.monitor(kvStore.crashed, log, "kvStore")

    configStore.storeConfigIfNotExist()
    jobs.addSubscriber {
      case (before, Some(after)) if before.map(_.version).getOrElse(0) != after.version =>
        // version changed?
        import context.dispatcher
        taskStore.save(after.pState).map(_ => PersistSuccess(after.id, after.version)) pipeTo self
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
      val behaviorSet = new JobBehavior(secrets, log, { () => frameworkId }, { () => cephConfig })
      _taskFSM = new JobFSM(jobs,
        log = log,
        behaviorSet = behaviorSet,
        setTimer = { (jobId, timerName, duration) =>
          import context.dispatcher
          context.system.scheduler.scheduleOnce(duration) {
            self ! JobTimer(jobId, timerName)
          }},
        revive = { () => throttledRevives.offer(()) },
        killTask = { (taskId: String) =>
          frameworkActor ! FrameworkActor.KillTask(newTaskId(taskId))
        }
      )

      log.info("InitialState: persistentTaskStates count = {}, fId = {}", persistentTaskStates.length, fId)
        cephConfig = _cephConfig
      frameworkId = fId

      persistentTaskStates.
        map { p =>
          Job.fromState(p, defaultBehavior = taskFSM.defaultBehavior)
        }.
        foreach(taskFSM.initialize)

      unstashAll()
      startReconciliation()
    case _ =>
      stash()
  }


  def startReconciliation(): Unit = {
    case object ReconcileTimeout

    var taskIdsForReconciliation: Set[String] =
      jobs.values.
        filter(_.slaveId.nonEmpty).
        flatMap { _.taskId }(breakOut)

    if (taskIdsForReconciliation.isEmpty) {
      log.info("Skipping reconciliation; no known tasks to reconcile")
      frameworkActor ! FrameworkActor.Reconcile(Nil)
      context.become(ready)
      return ()
    }

    log.info("Beginning reconciliation")
    val reconciliationTimer = context.system.scheduler.scheduleOnce(30.seconds, self, ReconcileTimeout)(context.dispatcher)
    var reconciledResult = List.empty[(Job, Protos.TaskStatus)]
    frameworkActor ! FrameworkActor.Reconcile(
      jobs.
        values.
        map { j => (j.taskId, j.slaveId) }.
        collect {
          case (Some(tid), Some(sid)) =>
            newTaskStatus(tid, sid, Protos.TaskState.TASK_LOST)
        }(breakOut))

    context.become {
      case ReconcileTimeout =>
        throw new Exception("timeout during reconciliation")
      case FrameworkActor.ResourceOffers(offers) =>
        offers.foreach { o =>
          frameworkActor ! FrameworkActor.DeclineOffer(o.getId, Some(5.seconds))
        }
      case FrameworkActor.StatusUpdate(taskStatus) =>
        val taskId = taskStatus.getTaskId.getValue
        jobs.getByTaskId(taskId) match {
          case Some(job) =>
            if (log.isDebugEnabled)
              log.debug("received stats update for {}: {}", job.id, taskStatus)
            else
              log.info("received status update for {}", job.id)
            taskIdsForReconciliation -= taskId
            reconciledResult = (job, taskStatus) :: reconciledResult

          case None =>
            log.info("received status update for unknown task {}; going to try and kill it", taskId)
            // The task is ours but we don't recognize it. Kill it.
            frameworkActor ! FrameworkActor.KillTask(taskStatus.getTaskId)
        }

        if (taskIdsForReconciliation.isEmpty) {
          reconciledResult.foreach { case (task, taskStatus) =>
            jobs.updateJob(task.withTaskStatus(taskStatus))
          }

          reconciliationTimer.cancel()
          unstashAll()
          log.info("reconciliation complete")
          frameworkActor ! FrameworkActor.Reconcile(Nil)
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

  def pendingOfferWithDeadline(offer: Protos.Offer, deadline:FiniteDuration = 5.seconds): PendingOffer = {
    val pendingOffer = PendingOffer(offer)
    context.system.scheduler.scheduleOnce(deadline) {
      pendingOffer.resultingOperationsPromise.trySuccess(Nil)
    }(context.dispatcher)
    pendingOffer
  }

  /** Looking at reservation labels, routes the offer to the appropriate
    *
    */
  def handleOffer(offer: Protos.Offer): Future[Iterable[Protos.Offer.Operation]] = {

    val reservedGroupings = offer.resources.groupBy { r =>
      r.reservation.
        flatMap(_.labels).
        map { labels =>
          (labels.get(Constants.ReservationIdLabel).map(UUID.fromString), labels.get(Constants.FrameworkIdLabel))
        }.
        getOrElse {
          (None, None)
        }
    }

    /* TODO - we could end up issuing the same set of resources twice for the same task in the case that a task is
    trying to grow in resources. That's not a supported use case right now. At the point it is supported, we can do
    mapping / grouping */
    val operations = reservedGroupings.map {
      case ((Some(reservationId), Some(OurFrameworkId())), resources)
          if jobs.containsReservationId(reservationId) =>
        val job = jobs.getByReservationId(reservationId).get
        val pendingOffer = pendingOfferWithDeadline(offer.withResources(resources))

        taskFSM.handleEvent(job, JobFSM.MatchedOffer(pendingOffer, None))

        pendingOffer.resultingOperations

      case ((Some(_), Some(OurFrameworkId())), resources) =>
        Future.successful(offerOperations.unreserveOffer(resources))
      case ((None, None), resources) =>
        val matchCandidateOffer = offer.withResources(resources)
        val matchingJob = jobs.values.
          toStream.
          filter(_.readyForOffer).
          flatMap { task =>
            val selector = ResourceMatcher.ResourceSelector.any(Set("*", config.role))
            offerMatchers.get(task.role).
              flatMap { _(matchCandidateOffer, task, jobs.values) }.
              map { (_, task) }
          }.
          headOption

        matchingJob match {
          case Some((matchResult, job)) =>
            // TODO - we need to do something with this result
            val pendingOffer = pendingOfferWithDeadline(matchCandidateOffer)
            taskFSM.handleEvent(
              job.copy(wantingNewOffer = false),
              JobFSM.MatchedOffer(pendingOffer, Some(matchResult)))

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
    case FrameworkActor.StatusUpdate(taskStatus) =>
      jobs.getByTaskId(taskStatus.getTaskId.getValue) match {
        case Some(job) =>
          jobs.updateJob(job.withTaskStatus(taskStatus))
        case None =>
          if (TaskState.fromMesos(taskStatus.getState).isInstanceOf[TaskState.Active]) {
            log.info("Received running task status update for unknown taskId {}; killing", taskStatus.getTaskId.getValue)
            frameworkActor ! FrameworkActor.KillTask(taskStatus.getTaskId)
          }

      }

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
        case GetJobs =>
          sender ! jobs.all

        case UpdateGoal(id, goal) =>
          jobs.get(id) foreach {
            case task if task.pState.goal.isEmpty =>
              log.error("Unabled to update run goal for jobId {}; it is not ready", id)
            case task =>
              val nextTask = task.withGoal(Some(goal))
              jobs.updateJob(nextTask)
          }
      }

    case FrameworkActor.Connected =>
      startReconciliation()

    case ConfigUpdate(Some(newCfg)) =>
      cephConfig = newCfg
      applyConfiguration()
    case ConfigUpdate(None) =>
      log.warning("Ceph config went missing / unparseable. Changes not applied")
    case JobTimer(id, timerName) =>
      taskFSM.onTimer(id, timerName)
    case PersistSuccess(id, version) =>
      jobs.updatePersistence(id, version)
  }

  def applyConfiguration(): Unit = {
    val newTasks = List(
      JobRole.Monitor -> cephConfig.deployment.mon.count,
      JobRole.RGW -> cephConfig.deployment.rgw.count,
      JobRole.OSD -> cephConfig.deployment.osd.count).
      flatMap {
        case (role, size) =>
          val roleTasks = jobs.values.filter( _.role == role)
          val newCount = Math.max(0, size - roleTasks.size)
          Stream.
            continually { Job.forRole(role, taskFSM.defaultBehavior) }.
            take(newCount)
      }.toList

    if (log.isInfoEnabled) {
      val newDesc = newTasks.groupBy(_.role).map { case (r, vs) => s"${r} -> ${vs.length}" }.mkString(", ")
      log.info("added {} as a result of config update", newDesc)
    }

    newTasks.foreach(taskFSM.initialize)
    offerMatchers = offerMatchFactory(cephConfig)

    if (jobs.values.exists(_.wantingNewOffer)) {
      log.info("matchers were updated. Scheduling revive")
      throttledRevives.offer(())
    }
  }
}
