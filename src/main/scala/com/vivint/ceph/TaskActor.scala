package com.vivint.ceph

import akka.actor.{ Actor, ActorContext, ActorLogging, ActorRef, Cancellable, FSM, Kill, PoisonPill, Props, Stash }
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
import com.vivint.ceph.kvstore.{KVStore, CrashingKVStore}
import com.vivint.ceph.model._
import scala.collection.breakOut
import scala.collection.JavaConverters._
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
  var frameworkId : FrameworkID = _
  var _behaviorSet: NodeBehavior = _
  implicit def behaviorSet: NodeBehavior =
    if (_behaviorSet == null)
      throw new IllegalStateException("tried to initialize a behaviorSet before behaviorSet was initializied")
    else
      _behaviorSet

  var nodes: Map[String, NodeState] = Map.empty
  var cephConfig: CephConfig = _

  val config = inject[AppConfiguration]
  val configStore = ConfigStore(kvStore)
  val offerMatchFactory = new MasterOfferMatchFactory
  var offerMatchers: Map[NodeRole.EnumVal, OfferMatchFactory.OfferMatcher] = Map.empty

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

  lib.FutureMonitor.monitor(result, "configuration stream")
  lib.FutureMonitor.monitor(kvStore.crashed, "kvStore")

  val throttledRevives = Source.queue[Unit](1, OverflowStrategy.dropTail).
    throttle(1, 5.seconds, 1, ThrottleMode.shaping).
    to(Sink.foreach({ _ =>
      frameworkActor ! FrameworkActor.ReviveOffers
    })).
    run

  override def preStart(): Unit = {
    import context.dispatcher

    configStore.storeConfigIfNotExist()

    log.info("pulling initial state for TaskActor")
    def logging[T](f: Future[T], desc: String): Future[T] = {
      log.debug(s"${desc} : pulling state")
      f.onComplete {
        case Success(_) => log.debug("{} : success", desc)
        case Failure(ex) =>
          log.error(ex, "{}: failure", desc)
          self ! PoisonPill
      }
      f
    }

    val initialState = tSequence(
      logging(taskStore.getNodes, "taskStore.getNodes"),
      logging(frameworkIdStore.get, "frameworkIdStore.get"),
      logging(ClusterSecretStore.createOrGenerateSecrets(kvStore), "secrets"),
      logging(deployConfigF, "deploy config")
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
      log.debug("{} - sending event {}", node.taskId, event.getClass.getName)

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
    log.debug("{} - processing directive response action {}", node.taskId, action.getClass.getName)
    action match {
      case NodeFSM.Hold(offer, resourceMatch) =>
        // Decline existing held offer
        node.heldOffer.foreach {
          case (pending, _) => pending.resultingOperationsPromise.trySuccess(Nil)
        }
        node.copy(heldOffer = Some((offer, resourceMatch)))
      case NodeFSM.Persist(data) =>
        node.copy(persistentState = Some(data))
      case NodeFSM.WantOffers =>
        throttledRevives.offer(())
        node.copy(
          wantingNewOffer = true,
          offerMatcher = offerMatchers.get(node.role))
      case NodeFSM.KillTask =>
        frameworkActor ! FrameworkActor.KillTask(newTaskId(node.taskId))
        node
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
      _behaviorSet = new NodeBehavior(secrets, { () => frameworkId }, { () => cephConfig })
      log.info("InitialState: persistentNodeStates count = {}, fId = {}", persistentNodeStates.length, fId)
        cephConfig = _cephConfig
        frameworkId = fId
      val newNodes = persistentNodeStates.map { p =>
        initializeBehavior(NodeState.fromState(p))
      }
      newNodes.foreach(updateNode)

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
      nodes.values.flatMap { _.taskStatus.map(_.getTaskId.getValue) }(breakOut)
    if (taskIdsForReconciliation.isEmpty) {
      log.info("Skipping reconciliation; no known tasks to reconcile")
      context.become(ready)
      return ()
    }
    log.info("Beginning reconciliation")
    reconciliationTimer = context.system.scheduler.scheduleOnce(30.seconds, self, ReconcileTimeout)(context.dispatcher)
    var reconciledResult = List.empty[(NodeState, TaskStatus)]
    frameworkActor ! FrameworkActor.Reconcile(nodes.values.flatMap(_.taskStatus)(breakOut))
    context.become {
      case ReconcileTimeout =>
        throw new Exception("timeout during reconciliation")
      case FrameworkActor.ResourceOffers(offers) =>
        offers.foreach { o =>
          frameworkActor ! FrameworkActor.DeclineOffer(o.getId, Some(5.seconds))
        }
      case FrameworkActor.StatusUpdate(taskStatus) =>
        val taskId = taskStatus.getTaskId.getValue
        nodes.get(taskId) match {
          case Some(node) =>
            if (log.isDebugEnabled)
              log.debug("received stats update {}", taskStatus)
            else
              log.info("received status update for {}", taskId)
            taskIdsForReconciliation -= taskId
            reconciledResult = (node, taskStatus) :: reconciledResult

          case None =>
            log.info("received status update for unknown task {}; going to try and kill it", taskId)
            // The task is ours but we don't recognize it. Kill it.
            frameworkActor ! FrameworkActor.KillTask(taskStatus.getTaskId)
        }

        if (taskIdsForReconciliation.isEmpty) {
          reconciledResult.foreach { case (node, taskStatus) =>
            updateNode(
              processEvents(
                node.copy(taskStatus = Some(taskStatus)),
                List(NodeFSM.NodeUpdated(node))))
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
    def unapply(fId: FrameworkID): Boolean = {
      fId == frameworkId
    }
    def unapply(fId: String) : Boolean = {
      fId == frameworkId.getValue
    }
  }

  /** Given an updated node status, persists if persistance has changed (further mondifying the nextVersion).
    * Returns the modified version.
    *
    * TODO - Extract and lock down valid operations. Nobody should be able to update the node state without going
    * through this method.
    */
  def updateNode(update: NodeState): NodeState = {
    val nextNode =
      if (nodes.get(update.taskId).flatMap(_.persistentState) != update.persistentState) {
        import context.dispatcher
        val nextVersion = update.version + 1
        taskStore.save(update.pState).map(_ => PersistSuccess(update.taskId, nextVersion)) pipeTo self
        update.copy(
          version = nextVersion)
      } else {
        update
      }

    if (log.isDebugEnabled)
    log.debug("node updated: {}", model.PlayJsonFormats.NodeStateWriter.writes(nextNode))
    nodes = nodes.updated(update.taskId, nextNode)
    nextNode
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

    /* TODO - we could end up issuing the same set of resources twice for the same node in the case that a node is
    trying to grow in resources. That's not a supported use case right now. At the point it is supported, we can do
    mapping / grouping */
    val operations = reservedGroupings.map {
      case ((Some(taskId), Some(OurFrameworkId())), resources)
          if nodes.get(taskId).flatMap(_.slaveId).contains(offer.getSlaveId.getValue) =>
        println("********************************************************************************")
        println(s"Le resident offer ${offer}")
        val node = nodes(taskId)
        println(model.PlayJsonFormats.NodeStateWriter.writes( node))
        val pendingOffer = PendingOffer(offer.withResources(resources))

        updateNode(
          processEvents(node, List(NodeFSM.MatchedOffer(pendingOffer, None))))

        pendingOffer.resultingOperations

      case ((Some(_), Some(OurFrameworkId())), resources) =>
        Future.successful(offerOperations.unreserveOffer(resources))
      case ((None, None), resources) =>
        val matchCandidateOffer = offer.withResources(resources)
        val matchingNode = nodes.values.
          toStream.
          filter(_.readyForOffer).
          flatMap { node =>
            val selector = ResourceMatcher.ResourceSelector.any(Set("*", config.role))
            node.offerMatcher.
              flatMap { _(matchCandidateOffer, node, nodes.values) }.
              map { (_, node) }
          }.
          headOption

        matchingNode match {
          case Some((matchResult, node)) =>
            // TODO - we need to do something with this result
            val pendingOffer = PendingOffer(matchCandidateOffer)
            updateNode {
              processEvents(
                node.copy(wantingNewOffer = false, offerMatcher = None),
                NodeFSM.MatchedOffer(pendingOffer, Some(matchResult)) :: Nil)}
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
      case FrameworkActor.StatusUpdate(taskStatus) if nodes contains taskStatus.getTaskId.getValue =>
        val priorState = nodes(taskStatus.getTaskId.getValue)
        val nextState = priorState.copy(taskStatus = Some(taskStatus))
        updateNode(
          processEvents(nextState, List(NodeFSM.NodeUpdated(priorState))))

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
          sender ! nodes

        case UpdateGoal(taskId, goal) =>
          nodes.get(taskId) foreach {
            case node if node.persistentState.isEmpty || node.pState.goal.isEmpty =>
              log.error("Unabled to update run goal for taskId {}; it is not ready", taskId)
            case node =>
              val nextNode = node.copy(persistentState = Some(node.pState.copy(goal = Some(goal))))
              updateNode(
                processEvents(
                  nextNode, List(NodeFSM.NodeUpdated(node))))
          }
      }

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

  def applyConfiguration(): Unit = {
    val monTasks = nodes.values.filter ( _.role == NodeRole.Monitor)
    val newMonitorCount = Math.max(0, cephConfig.deployment.mon.count - monTasks.size)
    val newMonitors = Stream.
      continually { NodeState.forRole(NodeRole.Monitor) }.
      take(newMonitorCount).
      map(initializeBehavior).
      toList

    val cephTasks = nodes.values.filter (_.role == NodeRole.OSD)
    val newOSDCount = Math.max(0, cephConfig.deployment.osd.count - cephTasks.size)
    val newOSDs = Stream.
      continually { NodeState.forRole(NodeRole.OSD) }.
      take(newOSDCount).
      map(initializeBehavior).
      toList

    log.info("added {} new monitors, {} new OSDs as a result of config update", newMonitors.length, newOSDs.length)

    (newMonitors ++ newOSDs).map(updateNode)
    offerMatchers = offerMatchFactory(cephConfig)

    var matchersUpdated = false
    nodes.foreach { case (taskId, node) =>
      if (node.wantingNewOffer) {
        matchersUpdated = true
        updateNode(
          node.copy(offerMatcher = offerMatchers.get(node.role)))
      }
    }
    if (matchersUpdated) {
      log.info("matchers were updated. Scheduling revive")
      throttledRevives.offer(())
    }
  }
}
