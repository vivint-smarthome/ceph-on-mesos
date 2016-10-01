package org.vivint.ceph

import akka.actor.{ Actor, ActorContext, ActorLogging, ActorRef, Cancellable, FSM, Kill, Props, Stash }
import akka.pattern.pipe
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{ Flow, Keep, Sink }
import java.util.concurrent.TimeoutException
import mesosphere.marathon.state.{ PersistentVolume, PersistentVolumeInfo }
import lib.FutureHelpers.tSequence
import org.apache.mesos.Protos._
import org.slf4j.LoggerFactory
import org.vivint.ceph.kvstore.{KVStore, CrashingKVStore}
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

  case class InitialState(
    nodes: Seq[CephNode],
    frameworkId: FrameworkID,
    secrets: ClusterSecrets,
    config: CephConfig)
  case class ConfigUpdate(deploymentConfig: Option[CephConfig])
  case class NodeTimer(taskId: String, payload: Any)
  case class PersistSuccess(taskId: String, version: Long)
  case class NodeUpdated(previousVersion: CephNode, nextVersion: CephNode)

  val log = LoggerFactory.getLogger(getClass)
}

class TaskActor(implicit val injector: Injector) extends Actor with ActorLogging with Stash {
  import TaskActor._
  val kvStore = CrashingKVStore(inject[KVStore])
  val taskStore = TaskStore(kvStore)
  val frameworkActor = inject[ActorRef](classOf[FrameworkActor])
  implicit val materializer = ActorMaterializer()
  val frameworkIdStore = inject[FrameworkIdStore]
  import ProtoHelpers._
  var frameworkId : String = _
  implicit var behaviorSet: NodeBehavior = _

  var nodes: Map[String, NodeState] = Map.empty
  var cephConfig: CephConfig = _

  val config = inject[AppConfiguration]
  val configStore = ConfigStore(kvStore)

  val getFirstConfigUpdate =
    Flow[Option[CephConfig]].
      collect { case Some(cfg) => cfg }.
      toMat(Sink.head)(Keep.right)

  val ((configStream, deployConfigF), result) =
    configStore.stream.
    alsoToMat(getFirstConfigUpdate)(Keep.both).
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

    configStore.storeConfigIfNotExist()

    tSequence(
      taskStore.getNodes,
      frameworkIdStore.get,
      ClusterSecretStore.createOrGenerateSecrets(kvStore),
      deployConfigF
    ).
      map(InitialState.tupled).
      pipeTo(self)
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
    processDirective(node,
      node.behavior.initialize(node, nodes))
  }

  def receive = {
    case InitialState(persistentNodeStates, fId, secrets, _cephConfig) =>
      cephConfig = _cephConfig
      frameworkId = fId.getValue
      val newNodes = persistentNodeStates.map { p =>
        initializeBehavior(NodeState.fromState(p))
      }
      nodes = newNodes.map { node => node.taskId -> node }(breakOut)
      behaviorSet = new NodeBehavior(secrets, { () => cephConfig })

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

    case ConfigUpdate(Some(newCfg)) =>
      cephConfig = newCfg
      applyConfiguration()
    case ConfigUpdate(None) =>
      throw new RuntimeException("Missing ceph config. Crashing task actor.")
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


  def applyConfiguration(): Unit = {
    val monTasks = nodes.values.filter ( _.role == "mon") // TODO introduce constant
    val newMonitorCount = Math.max(0, cephConfig.deployment.mon.count - monTasks.size)
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
            cephConfig.deployment.mon.disk,
            `type` = cephConfig.deployment.mon.disk_type),
          Volume.Mode.RW)

        // TODO - if matching reserved resources set matchers appropriately
        val matchers = List(
          new ScalarResourceMatcher(
            protos.Resource.CPUS, cephConfig.deployment.mon.cpus, selector, ScalarMatchResult.Scope.NoneDisk),
          new ScalarResourceMatcher(
            protos.Resource.MEM, cephConfig.deployment.mon.mem, selector, ScalarMatchResult.Scope.NoneDisk),
          new DiskResourceMatcher(
            selector, 0.0, List(volume), ScalarMatchResult.Scope.IncludingLocalVolumes),
          new lib.SinglePortMatcher(
            selector))
        nodes = nodes.updated(taskId, node.copy(offerMatchers = matchers))
      }
    }
  }
}
