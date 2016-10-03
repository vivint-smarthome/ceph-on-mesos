package org.vivint.ceph

import akka.actor.{ Actor, ActorContext, ActorLogging, ActorRef, Cancellable, FSM, Kill, Props, Stash }
import akka.pattern.pipe
import akka.stream.{ ActorMaterializer, OverflowStrategy, ThrottleMode }
import akka.stream.scaladsl.{ Flow, Keep, Sink, Source }
import java.util.concurrent.TimeoutException
import lib.FutureHelpers.tSequence
import mesosphere.marathon.state.{ PersistentVolume, PersistentVolumeInfo }
import mesosphere.mesos.matcher._
import mesosphere.mesos.protos
import org.apache.mesos.Protos._
import org.slf4j.LoggerFactory
import org.vivint.ceph.kvstore.{KVStore, CrashingKVStore}
import org.vivint.ceph.model._
import scala.collection.breakOut
import scala.collection.JavaConverters._
import scala.collection.immutable.{Iterable, Seq}
import scala.concurrent.Future
import scala.concurrent.duration._
import scaldi.Injectable._
import scaldi.Injector

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
  case class NodeTimer(actions: String, payload: Any)
  case class PersistSuccess(taskId: String, version: Long)
  case class NodeUpdated(previousVersion: CephNode, nextVersion: CephNode)

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

  val ((configStream, deployConfigF), result) = configStore.stream.
    dropWhile(_.isEmpty). // Handles initial bootstrap
    alsoToMat(getFirstConfigUpdate)(Keep.both).
    map(ConfigUpdate).
    toMat(Sink.foreach(self ! _))(Keep.both).
    run

  result.
    onFailure { case ex: Throwable =>
      log.error(ex, "Unexpected error in config stream")
      self ! Kill
    }(context.dispatcher)

  val throttledRevives = Source.queue[Unit](1, OverflowStrategy.dropTail).
    throttle(1, 5.seconds, 1, ThrottleMode.shaping).
    to(Sink.foreach({ _ =>
      frameworkActor ! FrameworkActor.ReviveOffers
    })).
    run

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
    throttledRevives.complete()
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
      case Some((offer, resourceMatch)) =>
        processEvents(
          node.copy(heldOffer = None),
          NodeFSM.MatchedOffer(offer, resourceMatch) :: Nil)
      case None =>
        node
    }
  }

  final def processAction(node: NodeState, action: NodeFSM.Action): NodeState = {
    action match {
      case NodeFSM.Hold(offer, resourceMatch) =>
        // Decline existing held offer
        node.heldOffer.foreach {
          case (pending, _) => pending.resultingOperationsPromise.trySuccess(Nil)
        }
        node.copy(heldOffer = Some((offer, resourceMatch)))
      case NodeFSM.Persist(data) =>
        if (node.persistentState == Some(data))
          node
        else {
          import context.dispatcher
          val nextVersion = node.version + 1
          taskStore.save(data).map(_ => PersistSuccess(node.taskId, nextVersion)) pipeTo self
          node.copy(
            version = nextVersion,
            persistentState = Some(data))
        }
      case NodeFSM.WantOffers =>
        throttledRevives.offer(())
        node.copy(
          wantingNewOffer = true,
          offerMatchers = calcMatchers(node))
      case NodeFSM.OfferResponse(pendingOffer, operations) =>
        pendingOffer.resultingOperationsPromise.success(operations.toList)
        node
    }
  }

  final def processDirective(node: NodeState, directive: NodeFSM.Directive): NodeState = {
    val nodeAfterAction = directive.action.foldLeft(node)(processAction)

    directive.transition match {
      case Some(nextBehaviorFactory) =>
        node.behavior.teardown()
        val nextBehavior = nextBehaviorFactory(node.taskId, context)
        log.info("node {}: Transition {} -> {}", node.taskId, node.behavior.name, nextBehavior.name)
        processHeldEvents(
          initializeBehavior(nodeAfterAction.copy(behavior = nextBehavior)))

      case None =>
        nodeAfterAction
    }
  }

  final def initializeBehavior(node: NodeState): NodeState = {
    log.info("node {}: Initializing behavior {}", node.taskId, node.behavior.name)
    processDirective(node,
      node.behavior.initialize(node, nodes))
  }

  def receive = {
    case iState @ InitialState(persistentNodeStates, fId, secrets, _cephConfig) =>
      log.info("InitialState: persistentNodeStates count = {}, fId = {}", persistentNodeStates.length, fId)
        cephConfig = _cephConfig
        frameworkId = fId.getValue
      val newNodes = persistentNodeStates.map { p =>
        initializeBehavior(NodeState.fromState(p))
      }
      nodes = newNodes.map { node => node.taskId -> node }(breakOut)
      behaviorSet = new NodeBehavior(secrets, { () => frameworkId }, { () => cephConfig })

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
    if (taskIdsForReconciliation.isEmpty) {
      log.info("Skipping reconciliation; no known tasks to reconcile")
      context.become(ready)
    } else {
      log.info("Beginning reconciliation")
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

  /** Looking at reservation labels, routes the offer to the appropriate
    *
    */
  def handleOffer(offer: Offer): Future[Iterable[Offer.Operation]] = {

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

    val frameworkId = this.frameworkId

    /* TODO - we could end up issuing the same set of resources twice for the same node in the case that a node is
    trying to grow in resources. That's not a supported use case right now. At the point it is supported, we can do
    mapping / grouping */
    val operations = reservedGroupings.map {
      case ((Some(taskId), Some(`frameworkId`)), resources) if nodes.contains(taskId) =>
        val node = nodes(taskId)
        val pendingOffer = PendingOffer(offer.withResources(resources))

        updateNode(
          processEvents(node, List(NodeFSM.MatchedOffer(pendingOffer, None))))

        pendingOffer.resultingOperations

      case ((Some(_), Some(`frameworkId`)), resources) =>
        Future.successful(offerOperations.unreserveOffer(resources))
      case (_, resources) =>
        val matchCandidateOffer = offer.withResources(resources)
        val matchingNode = nodes.values.
          toStream.
          filter(_.readyForOffer).
          flatMap { node =>
            val selector = ResourceMatcher.ResourceSelector.any(Set("*", config.role))
            ResourceMatcher.matchResources(matchCandidateOffer, node.offerMatchers, selector).
              map { (_, node) }

          }.
          headOption

        matchingNode match {
          case Some((matchResult, node)) =>
            // TODO - we need to do something with this result
            val pendingOffer = PendingOffer(matchCandidateOffer)
            updateNode {
              processEvents(
                node.copy(wantingNewOffer = false, offerMatchers = Nil),
                NodeFSM.MatchedOffer(pendingOffer, Some(matchResult)) :: Nil)}
            pendingOffer.resultingOperations
          case _ =>
            Future.successful(Nil)
        }
    }
    import context.dispatcher
    Future.sequence(operations).map(_.flatten)
  }

  def ready: Receive = (
    {
      case FrameworkActor.ResourceOffers(offers) =>
        offers.foreach { offer =>
          import context.dispatcher
          handleOffer(offer).
            map { ops =>
              if (ops.isEmpty)
                FrameworkActor.DeclineOffer(offer.getId, Some(30.seconds))
              else
                FrameworkActor.AcceptOffer(offer.getId, ops.toList)
            }.
            pipeTo(frameworkActor)
        }
        // val offerOpsFuture = offers.map(handleOffer)
        // val rejectedOffers =
        // rejectedOffers.foreach { o =>
        //   frameworkActor ! FrameworkActor.DeclineOffer(o.getId, Some(30.seconds))
        // }

        // TODO
        // if reservation: known? route to task actor; unknown? destroy / free...
        /* match resource, execute reservation and launch actor. If reserved resource doesn't get re-offered after a
         period of time then crash. Watch for death and put actor back in to pending stage. Task must store
         reservationId, which must be unique per reservation */
        // stay

    }: Receive
  ).
    orElse(default)

  def default: Receive = {
    case FrameworkActor.Connected =>
      startReconciliation()

    case ConfigUpdate(Some(newCfg)) =>
      cephConfig = newCfg
      applyConfiguration()
    case ConfigUpdate(None) =>
      log.warning("Ceph config went missing / unparseable. Changes not applied")
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

  def calcMatchers(node: NodeState) = {
    val selector = ResourceMatcher.ResourceSelector.any(Set("*", config.role))

    val volume = PersistentVolume.apply(
      "state",
      PersistentVolumeInfo(
        cephConfig.deployment.mon.disk,
        `type` = cephConfig.deployment.mon.disk_type),
      Volume.Mode.RW)

    // TODO - if matching reserved resources set matchers appropriately
    List(
      new ScalarResourceMatcher(
        protos.Resource.CPUS, cephConfig.deployment.mon.cpus, selector, ScalarMatchResult.Scope.NoneDisk),
      new ScalarResourceMatcher(
        protos.Resource.MEM, cephConfig.deployment.mon.mem, selector, ScalarMatchResult.Scope.NoneDisk),
      new DiskResourceMatcher(
        selector, 0.0, List(volume), ScalarMatchResult.Scope.IncludingLocalVolumes),
      new lib.SinglePortMatcher(
        selector))
  }

  def applyConfiguration(): Unit = {
    val monTasks = nodes.values.filter ( _.role == "mon") // TODO introduce constant
    val newMonitorCount = Math.max(0, cephConfig.deployment.mon.count - monTasks.size)
    val newMonitors = Stream.
      continually { NodeState.forRole("mon") }.
      map(initializeBehavior).
      take(newMonitorCount).
      toList
    log.info("added {} new monitors as a result of config update", newMonitors.length)

    nodes = nodes ++ newMonitors.map { m => m.taskId -> m }

    var matchersUpdated = false
    nodes.foreach { case (taskId, node) =>
      if (node.wantingNewOffer) {
        matchersUpdated = true
        nodes = nodes.updated(taskId, node.copy(offerMatchers = calcMatchers(node)))
      }
    }
    if (matchersUpdated) {
      log.info("matchers were updated. Scheduling revive")
      throttledRevives.offer(())
    }

  }
}
