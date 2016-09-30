package org.vivint.ceph
import akka.Done
import akka.actor.{ Actor, ActorContext, ActorLogging, ActorRef, Cancellable, FSM, Kill, Props, Stash }
import akka.pattern.pipe
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{ Flow, Keep, Sink }
import java.nio.charset.StandardCharsets.UTF_8
import java.util.UUID
import java.util.concurrent.TimeoutException
import mesosphere.marathon.state.{ PersistentVolume, PersistentVolumeInfo }
import org.apache.mesos.Protos._
import org.slf4j.LoggerFactory
import org.vivint.ceph.kvstore.{KVStore, CrashingKVStore}
import scala.annotation.tailrec
import scaldi.Injectable._
import scaldi.Injector
import scala.concurrent.duration._
import scala.collection.breakOut
import org.vivint.ceph.model._
import mesosphere.mesos.matcher.ResourceMatcher

object TaskActor {
  sealed trait State
  case object Initializing extends State
  case object Disconnected extends State
  case object Reconciling extends State
  case object Ready extends State

  def makeTaskId(role: String, cluster: String, id: UUID): String =
    s"${role}#${cluster}#${id.toString}"
  case class NodeState(
    id: UUID,
    cluster: String,
    role: String,
    version: Long = 0,
    persistentState: Option[CephNode] = None,
    behavior: Behavior,
    persistentVersion: Long = 0,
    wantingNewOffer: Boolean = false,
    heldOffer: Option[Offer] = None,
    offerMatchers: List[ResourceMatcher] = Nil,
    taskStatus: Option[TaskStatus] = None
  ) {
    lazy val taskId = makeTaskId(role, cluster, id)
    taskStatus.foreach { s =>
      require(s.getTaskId.getValue == taskId, "Critical error - TaskStatus must match generated node state")
    }
  }
  object NodeState {
    def newNode(id: UUID, cluster: String, role: String, persistentState: Option[CephNode]): NodeState = {
      val taskId = makeTaskId(role = role, cluster = cluster, id = id)
      val taskStatus = for {
        p <- persistentState
        slaveId <- p.slaveId
      } yield ProtoHelpers.newTaskStatus(taskId, slaveId)

      NodeState(
        id = id,
        cluster = cluster,
        role = role,
        persistentState = persistentState,
        behavior = InitializeLogic(taskId),
        taskStatus = taskStatus)
    }

    def forRole(role: String): NodeState = {
      newNode(
        id = UUID.randomUUID,
        cluster = Constants.DefaultCluster,
        role = role,
        persistentState = None)
    }


    def fromState(state: CephNode): NodeState = {
      newNode(
        id = state.id,
        cluster = state.cluster,
        role = state.role,
        persistentState = Some(state))
    }
  }



  // case class Data(
  //   nodes: Map[String, NodeState] = Map.empty,
  //   pendingReconcile: Set[String] = Set.empty,
  //   deploymentConfig: Option[DeploymentConfig] = None
  // )

  case class InitialState(nodes: Seq[CephNode], frameworkId: FrameworkID)
  case class ConfigUpdate(deploymentConfig: Option[DeploymentConfig])
  case class NodeTimer(taskId: String, payload: Any)
  case class PersistSuccess(taskId: String, version: Long)

  val log = LoggerFactory.getLogger(getClass)
  val configParsingFlow = Flow[Option[Array[Byte]]].
    map {
      case Some(bytes) =>
        try Some(DeploymentConfigHelper.parse(new String(bytes, UTF_8)))
        catch { case ex: Throwable =>
          log.error("Error parsing configuration. Task actor will remain idle", ex)
          None
        }
      case None =>
        log.error("No configuration detected.")
        None
    }
}

class TaskActor(implicit val injector: Injector) extends Actor with ActorLogging with Stash {
  import TaskActor._
  val kvStore = CrashingKVStore(inject[KVStore])
  val taskStore = TaskStore(kvStore)
  val frameworkStore = TaskStore(kvStore)
  val frameworkActor = inject[ActorRef](classOf[FrameworkActor])
  implicit val materializer = ActorMaterializer()
  val frameworkIdStore = inject[FrameworkIdStore]
  import ProtoHelpers._
  var frameworkId : String = _

  var nodes: Map[String, NodeState] = Map.empty
  var deploymentConfig: Option[DeploymentConfig] = None

  val config = inject[AppConfiguration]


  val (configStream, result) = kvStore.watch("config.json").
    via(configParsingFlow).
    map(ConfigUpdate).
    toMat(Sink.foreach(self ! _))(Keep.both).
    run

  result.
    onFailure { case ex: Throwable =>
      log.error(ex, "Unexpected error in config stream")
      self ! Kill
    }(context.dispatcher)

  override def preStart(): Unit = {
    import context.dispatcher
    taskStore.getNodes.
      zip(frameworkIdStore.get).
      map(InitialState.tupled).pipeTo(self)
  }

  override def postStop(): Unit = {
    configStream.cancel()
  }

  final def processEvents(node: NodeState, events: List[NodeFSM.Event]): NodeState = events match {
    case event :: rest =>
      processEvents(
        processDirective(
          node,
          node.behavior.submit(event, node, nodes)),
        rest)
    case Nil =>
      node
  }

  final def processHeldEvents(node: NodeState): NodeState = {
    node.heldOffer match {
      case Some(offer) =>
        processEvents(
          node.copy(heldOffer = None),
          NodeFSM.MatchedOffer(offer) :: Nil)
      case None =>
        node
    }
  }

  final def processDirective(node: NodeState, directive: NodeFSM.Directive): NodeState = {
    val nodeAfterAction = directive.action match {
      case Some(NodeFSM.Hold(offer)) =>
        node.copy(heldOffer = Some(offer))
      case Some(NodeFSM.Persist(data)) =>
        import context.dispatcher
        val nextVersion = node.version + 1
        taskStore.save(data).map(_ => PersistSuccess(node.taskId, nextVersion)) pipeTo self
        node.copy(
          version = nextVersion,
          persistentState = Some(data))
      case None =>
        node
    }

    directive.transition match {
      case Some(nextBehaviorFactory) =>
        node.behavior.teardown()
        val nextBehavior = nextBehaviorFactory(node.taskId, context)
        processHeldEvents(
          initializeBehavior(nodeAfterAction.copy(behavior = nextBehavior)))

      case None =>
        nodeAfterAction
    }
  }

  final def initializeBehavior(node: NodeState): NodeState = {
    processDirective(node, node.behavior.initialize(node, nodes))
  }

  def receive = {
    case InitialState(persistentNodeStates, fId) =>
      frameworkId = fId.getValue
      val newNodes = persistentNodeStates.map { p =>
        initializeBehavior(NodeState.fromState(p))
      }
      nodes = newNodes.map { node => node.taskId -> node }(breakOut)

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
    val taskIdsForReconciliation: Set[String] =
      nodes.values.flatMap { _.taskStatus.map(_.getTaskId.getValue) }(breakOut)
    if (taskIdsForReconciliation.isEmpty)
      context.become(ready)
    else {
      reconciliationTimer = context.system.scheduler.scheduleOnce(30.seconds, self, ReconcileTimeout)(context.dispatcher)
      frameworkActor ! FrameworkActor.Reconcile(
        nodes.values.flatMap(_.taskStatus)(breakOut))
      context.become(
        reconciling(taskIdsForReconciliation))
    }
  }

  def reconciling(taskIdsForReconciliation: Set[String]): Receive = (
    {
      case ReconcileTimeout =>
        throw new TimeoutException("Timed out while reconciling")
      case FrameworkActor.ResourceOffers(offers) =>
        offers.foreach { o =>
          frameworkActor ! FrameworkActor.DeclineOffer(o.getId, Some(5.seconds))
        }
      case FrameworkActor.StatusUpdate(taskStatus) =>
        val taskId = taskStatus.getTaskId.getValue
        nodes.get(taskId) match {
          case Some(node) =>
            val nextState = node.copy(taskStatus = Some(taskStatus))
            val nextPending = taskIdsForReconciliation - taskId

            updateNode(
              processEvents(nextState, List(NodeFSM.NodeUpdated(node))))

            if (nextPending.isEmpty) {
              reconciliationTimer.cancel()
              unstashAll()
              context.become(ready)
            } else {
              context.become(reconciling(nextPending))
            }
          case None =>
            maybeKillUnknownTask(taskStatus)
        }
    }: Receive
  ).
    orElse(default).
    orElse(stashReceiver)

  def updateNode(node: NodeState): Unit =
    nodes = nodes.updated(node.taskId, node)

  def stashReceiver: Receive = { case _ => stash }

  def maybeKillUnknownTask(taskStatus: TaskStatus) = {
    if (taskStatus.getLabels.get(Constants.FrameworkIdLabel) == Some(frameworkId)) {
      // kill it
      ???
    }
  }

  def ready: Receive = {
    case FrameworkActor.ResourceOffers(offers) =>
      // TODO
      // if reservation: known? route to task actor; unknown? destroy / free...
      /* match resource, execute reservation and launch actor. If reserved resource doesn't get re-offered after a
       period of time then crash. Watch for death and put actor back in to pending stage. Task must store
       reservationId, which must be unique per reservation */
      // stay

  }


  def default: Receive = {
    case FrameworkActor.Connected =>
      startReconciliation()

    case ConfigUpdate(newCfg) =>
      deploymentConfig = newCfg
      applyConfiguration()
    case NodeTimer(taskId, payload) =>
      nodes.get(taskId) foreach { node =>
        updateNode(
          processEvents(node, List(NodeFSM.Timer(payload))))
      }
    case PersistSuccess(taskId, version) =>
      nodes.get(taskId) foreach { node =>
        updateNode(
          processEvents(
            node.copy(persistentVersion = Math.max(node.persistentVersion, version)),
            List(NodeFSM.NodeUpdated(node))))
      }
  }

  def processNodeEventLoop(events: List[NodeFSM.Event]) = {
  }


  def applyConfiguration(): Unit = deploymentConfig foreach { cfg =>
    val monTasks = nodes.values.filter ( _.role == "mon") // TODO introduce constant
    val newMonitorCount = Math.max(0, cfg.mon.count - monTasks.size)
    val newMonitors = Stream.
      continually { NodeState.forRole("mon") }.
      map(initializeBehavior).
      take(newMonitorCount)

    nodes = nodes ++ newMonitors.map { m => m.taskId -> m }

    nodes.foreach { case (taskId, node) =>
      if (node.wantingNewOffer) {
        import mesosphere.mesos.matcher._
        import mesosphere.mesos.protos

        val selector = ResourceMatcher.ResourceSelector.any(Set("*", config.role))

        val volume = PersistentVolume.apply(
          "state",
          PersistentVolumeInfo(
            cfg.mon.disk,
            `type` = cfg.mon.diskType),
          Volume.Mode.RW)

        // TODO - if matching reserved resources set matchers appropriately
        val matchers = List(
          new ScalarResourceMatcher(protos.Resource.CPUS, cfg.mon.cpus, selector, ScalarMatchResult.Scope.NoneDisk),
          new ScalarResourceMatcher(protos.Resource.MEM, cfg.mon.mem, selector, ScalarMatchResult.Scope.NoneDisk),
          new DiskResourceMatcher(selector, 0.0, List(volume), ScalarMatchResult.Scope.IncludingLocalVolumes),
          new lib.SinglePortMatcher(selector))
        nodes = nodes.updated(taskId, node.copy(offerMatchers = matchers))
      }
    }
  }
}
