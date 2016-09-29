package org.vivint.ceph

import akka.Done
import akka.actor.{ Actor, ActorContext, ActorLogging, ActorRef, FSM, Kill, Props, Stash }
import akka.pattern.pipe
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{ Keep, Sink }
import java.nio.charset.StandardCharsets.UTF_8
import java.util.UUID
import java.util.concurrent.TimeoutException
import mesosphere.marathon.state.{ PersistentVolume, PersistentVolumeInfo }
import org.apache.mesos.Protos._
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

  def makeTaskId(role: String, cluster: String, id: UUID): String =
    s"${role}#${cluster}#${id.toString}"
  case class NodeState(
    id: UUID,
    cluster: String,
    role: String,
    actor: ActorRef,
    persistentState: Option[CephNode],
    version: Long = 0,
    wantingOffer: Boolean = false,
    offerMatchers: List[ResourceMatcher] = Nil,
    taskStatus: Option[TaskStatus] = None
  ) {
    lazy val taskId = makeTaskId(role, cluster, id)
    taskStatus.foreach { s =>
      require(s.getTaskId.getValue == taskId, "Critical error - TaskStatus must match generated node state")
    }
  }

  def taskIdsForReconciliation(nodeStates: Iterable[NodeState]): Set[String] =
    nodeStates.flatMap { _.taskStatus.map(_.getTaskId.getValue) }(breakOut)

  case class Data(
    nodes: Map[String, NodeState] = Map.empty,
    pendingReconcile: Set[String] = Set.empty,
    deploymentConfig: Option[DeploymentConfig] = None
  )

  case class InitialState(nodes: Seq[CephNode], frameworkId: FrameworkID)
  case class ConfigUpdate(deploymentConfig: Option[DeploymentConfig])
}

class TaskActor(implicit val injector: Injector) extends FSM[TaskActor.State, TaskActor.Data] with Stash {
  import TaskActor._
  val kvStore = CrashingKVStore(inject[KVStore])
  val taskStore = TaskStore(kvStore)
  val frameworkStore = TaskStore(kvStore)
  val frameworkActor = inject[ActorRef](classOf[FrameworkActor])
  implicit val materializer = ActorMaterializer()
  val frameworkIdStore = inject[FrameworkIdStore]
  import ProtoHelpers._
  var frameworkId : String = _
  val config = inject[AppConfiguration]

  def newNode(id: UUID, cluster: String, role: String, persistentState: Option[CephNode]): NodeState = {
    val nodeActor = context.actorOf(Props(new NodeActor), s"node-${id}")
    val taskStatus = for {
      p <- persistentState
      slaveId <- p.slaveId
    } yield newTaskStatus(makeTaskId(role = role, cluster = cluster, id = id), slaveId)

    NodeState(
      id = id,
      cluster = cluster,
      role = role,
      actor = nodeActor,
      persistentState = persistentState,
      taskStatus = taskStatus
    )
  }

  def addNode(role: String): NodeState = {
    newNode(
      id = UUID.randomUUID,
      cluster = Constants.DefaultCluster,
      role = role,
      persistentState = None)
  }


  def newNodeFromState(state: CephNode): NodeState = {
    newNode(
      id = state.id,
      cluster = state.cluster,
      role = state.role,
      persistentState = Some(state))
  }

  val (configStream, result) = kvStore.watch("config.json").
    map {
      case Some(bytes) =>
        try Some(DeploymentConfigHelper.parse(new String(bytes, UTF_8)))
        catch { case ex: Throwable =>
          log.error(ex, "Error parsing configuration. Task actor will remain idle")
          None
        }
      case None =>
        log.error("No configuration detected.")
        None
    }.
    toMat(Sink.foreach(c => self ! ConfigUpdate(c)))(Keep.both).
    run

  result.
    onFailure { case ex: Throwable =>
      log.error(ex, "Unexpected error in config stream")
      self ! Kill
    }(context.dispatcher)

  startWith(Initializing, Data())

  override def preStart(): Unit = {
    import context.dispatcher
    taskStore.getNodes.
      zip(frameworkIdStore.get).
      map(InitialState.tupled).pipeTo(self)
  }

  override def postStop(): Unit = {
    configStream.cancel()
  }

  when(Initializing) {
    case Event(InitialState(persistentNodeStates, fId), data) =>
      frameworkId = fId.getValue
      val nodes = persistentNodeStates.map(newNodeFromState)
      val nextData = data.copy(
        nodes = nodes.map { node => node.taskId -> node }(breakOut),
        pendingReconcile = taskIdsForReconciliation(nodes))
      unstashAll()
      goto(Reconciling) using nextData
    case Event(_, _) =>
      stash()
      stay
  }

  when(Disconnected) {
    case Event(FrameworkActor.Connected, data) =>
      goto(Reconciling) using (
        data.copy(
          pendingReconcile = taskIdsForReconciliation(data.nodes.values)))
  }

  def transitionIfDone(nextData: Data) =
    if (nextData.pendingReconcile.isEmpty) {
      nextData.nodes.values.foreach { node => node.actor ! node }
      goto(Ready) using (nextData)
    } else {
      stay using (nextData)
    }

  def maybeKillUnknownTask(taskStatus: TaskStatus) = {
    if (taskStatus.getLabels.get(Constants.FrameworkIdLabel) == Some(frameworkId)) {
      // kill it
      ???
    }
  }

  when(Reconciling, 30.seconds) {
    case Event(StateTimeout, _) =>
      throw new TimeoutException("Timed out while reconciling")
    case Event(Done, data) =>
      transitionIfDone(data)
    case Event(FrameworkActor.ResourceOffers(offers), _) =>
      offers.foreach { o =>
        frameworkActor ! FrameworkActor.DeclineOffer(o.getId, Some(5.seconds))
      }
      stay
    case Event(FrameworkActor.StatusUpdate(taskStatus), data) =>
      val taskId = taskStatus.getTaskId.getValue
      data.nodes.get(taskId) match {
        case Some(node) =>
          val nextData = data.copy(
            nodes = data.nodes.updated(taskId, node.copy(taskStatus = Some(taskStatus))),
            pendingReconcile = data.pendingReconcile - taskId)
          node.actor ! nextData

          transitionIfDone(nextData)
        case None =>
          maybeKillUnknownTask(taskStatus)
          stay
      }
  }

  when(Ready) {
    case Event(FrameworkActor.ResourceOffers(offers), data) =>
      // TODO
      // if reservation: known? route to task actor; unknown? destroy / free...
      /* match resource, execute reservation and launch actor. If reserved resource doesn't get re-offered after a
        period of time then crash. Watch for death and put actor back in to pending stage. Task must store
        reservationId, which must be unique per reservation */
      stay
  }

  whenUnhandled {
    case Event(FrameworkActor.Connected, data) =>
      goto(Reconciling) using (data.copy(pendingReconcile = taskIdsForReconciliation(data.nodes.values)))

    case Event(ConfigUpdate(None), data) =>
      stay using data.copy(deploymentConfig = None)

    case Event(ConfigUpdate(Some(cfg)), data) =>
      val monTasks = data.nodes.values.filter ( _.role == "mon") // TODO introduce constant
      val newMonitorCount = Math.max(0, cfg.mon.count - monTasks.size)
      val newMonitors = Stream.
        continually { addNode("mon") }.
        take(newMonitorCount)
      newMonitors.foreach { node => node.actor ! node }

      // TODO - add offers if requested
      stay using data.copy(
        deploymentConfig = Some(cfg),
        nodes = applyConfiguration(
          data.nodes ++ newMonitors.map { n => n.taskId -> n },
          cfg))
  }

  def applyConfiguration(nodes: Map[String, NodeState], cfg: DeploymentConfig): Map[String, NodeState] = {
    nodes.map { case (id, node) =>
      if (node.wantingOffer) {
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
        id -> node.copy(offerMatchers = matchers)
      } else {
        id -> node
      }
    }(breakOut)
  }


  onTransition {
    case _ -> Reconciling =>
      if (nextStateData.pendingReconcile.isEmpty)
        self ! Done
      else
        frameworkActor ! FrameworkActor.Reconcile(
          this.nextStateData.nodes.values.flatMap(_.taskStatus)(breakOut))
  }
}
