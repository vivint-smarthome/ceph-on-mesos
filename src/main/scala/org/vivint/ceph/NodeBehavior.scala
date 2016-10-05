package org.vivint.ceph

import akka.actor.{ ActorContext, Cancellable }
import java.util.concurrent.atomic.AtomicInteger
import mesosphere.mesos.protos.TaskStatus
import org.apache.mesos.Protos
import org.vivint.ceph.model.{ NodeRole, RunState, CephNode, CephConfig, NodeState, ServiceLocation }
import NodeFSM._
import Behavior._
import scala.annotation.tailrec
import scala.concurrent.duration._
import model.ClusterSecrets
import scaldi.Injector
import scaldi.Injectable._
import mesosphere.mesos.protos.Resource.PORTS
import ProtoHelpers._
import java.util.Base64

class NodeBehavior(
  secrets: ClusterSecrets,
  frameworkId: () => Protos.FrameworkID,
  deploymentConfig: () => CephConfig)(implicit injector: Injector)
    extends BehaviorSet {

  val offerOperations = inject[OfferOperations]
  val configTemplates = inject[views.ConfigTemplates]
  val appConfig = inject[AppConfiguration]

  def decideWhatsNext(state: NodeState, fullState: Map[String, NodeState]): Directive = {
    import Directives._
    state.persistentState match {
      case None =>
        persist(state.inferPersistedState).
          withTransition(
            WaitForSync(decideWhatsNext))
      case Some(pState) if state.version != state.persistentVersion =>
        // try again; previous persistence must have timed out.
        persist(pState).
          withTransition(
            WaitForSync(decideWhatsNext))
      case Some(pState) =>
        if (pState.reservationConfirmed)
          transition(Running)
        else if (pState.slaveId.nonEmpty)
          transition(WaitForReservation)
        else
          transition(Matching)
    }
  }

  case class InitializeLogic(taskId: String, actorContext: ActorContext) extends Behavior {
    override def preStart(state: NodeState, fullState: Map[String, NodeState]): Directive = {
      decideWhatsNext(state: NodeState, fullState: Map[String, NodeState]): Directive
    }

    def handleEvent(event: Event, state: NodeState, fullState: Map[String, NodeState]): Directive =
      throw new IllegalStateException("handleEvent called on InitializeLogic")
  }

  case class WaitForSync(nextBehavior: DecideFunction)(val taskId: String, val actorContext: ActorContext) extends Behavior {
    override def preStart(state: NodeState, fullState: Map[String, NodeState]): Directive = {
      setBehaviorTimer("timeout", 30.seconds)
      stay
    }

    def handleEvent(event: Event, state: NodeState, fullState: Map[String, NodeState]): Directive = {
      event match {
        case Timer("timeout") =>
          nextBehavior(state, fullState)
        case Timer(_) =>
          stay
        case NodeUpdated(prior) =>
          if (state.persistentVersion < state.version)
            stay
          else
            nextBehavior(state, fullState)
        case MatchedOffer(offer, matchResult) =>
          hold(offer, matchResult)
      }
    }
  }

  case class Matching(taskId: String, actorContext: ActorContext) extends Behavior {
    override def preStart(state: NodeState, fullState: Map[String, NodeState]): Directive = {
      wantOffers
    }

    def handleEvent(event: Event, state: NodeState, fullState: Map[String, NodeState]): Directive = {
      event match {
        case Timer(_) =>
          stay
        case NodeUpdated(_) =>
          stay
        case MatchedOffer(pendingOffer, matchResult) =>
          val resources = pendingOffer.offer.resources
          if (resources.forall { r => r.hasReservation }) {

            val newState = state.inferPersistedState.copy(
              reservationConfirmed = true,
              slaveId = Some(pendingOffer.slaveId))
            persist(newState) andAlso hold(pendingOffer, matchResult) withTransition (Running)
          } else {
            matchResult match {
              case Some(result) =>
                offerResponse(
                  pendingOffer,
                  offerOperations.reserveAndCreateVolumes(frameworkId(), taskId, result)).
                  andAlso(
                    persist(
                      state.inferPersistedState.copy(slaveId = Some(pendingOffer.slaveId)))).
                  withTransition(WaitForReservation)
              case None =>
                offerResponse(pendingOffer, Nil) andAlso wantOffers
            }
          }
      }
    }
  }

  case class WaitForReservation(taskId: String, actorContext: ActorContext) extends Behavior {
    override def preStart(state: NodeState, fullState: Map[String, NodeState]): Directive = {
      setBehaviorTimer("timeout", 30.seconds)
      stay
    }

    def handleEvent(event: Event, state: NodeState, fullState: Map[String, NodeState]): Directive = {
      event match {
        case Timer("timeout") =>
          transition(Matching)
        case Timer(_) =>
          stay
        case MatchedOffer(pendingOffer, matchResult) =>
          if (pendingOffer.offer.resources.exists(_.hasReservation)) {
            val newState = state.inferPersistedState.copy(
              reservationConfirmed = true)
            println(s"newState = ${newState}")
            persist(newState).
              andAlso(hold(pendingOffer, matchResult)).
              withTransition(WaitForSync(decideWhatsNext))
          } else {
            hold(pendingOffer, matchResult).withTransition(Matching)
          }
        case NodeUpdated(_) =>
          stay
      }
    }
  }

  case class Sleep(duration: FiniteDuration, andThen: DecideFunction)(val taskId: String, val actorContext: ActorContext) extends Behavior {
    override def preStart(state: NodeState, fullState: Map[String, NodeState]): Directive = {
      setBehaviorTimer("wakeup", duration)
      stay
    }

    def handleEvent(event: Event, state: NodeState, fullState: Map[String, NodeState]): Directive = {
      event match {
        case Timer("wakeup") =>
          andThen(state, fullState)
        case Timer(_) | NodeUpdated(_) =>
          stay
        case MatchedOffer(offer, matchResult) =>
          hold(offer, matchResult)
      }
    }
  }


  case class Running(taskId: String, actorContext: ActorContext) extends Behavior {
    def reservationConfirmed(state:NodeState) =
      state.pState.reservationConfirmed

    def getMonitors(fullState: Map[String, NodeState]) =
      fullState.values.filter(_.role == NodeRole.Monitor).toList

    override def preStart(state: NodeState, fullState: Map[String, NodeState]): Directive = {
      if(! reservationConfirmed(state))
        throw new IllegalStateException("Can't go to running state without a confirmed reservation")

      /* TODO - move this logic to orchestrator */
      val monitors = getMonitors(fullState)
      lazy val runningMonitors = monitors.filter(_.runningState == Some(RunState.Running)).toList
      lazy val quorum = runningMonitors.length > (monitors.length / 2.0)
      def becomeRunning =
        persist(state.pState.copy(goal = state.pState.goal.orElse(Some(RunState.Running))))
      def poll =
        transition(Sleep(5.seconds, { (_, _) => transition(Running) }))

      state.role match {
        case NodeRole.Monitor =>
          if (runningMonitors.nonEmpty || (monitors.forall(_.pState.goal.isEmpty))) // am I the first or are others running?
            becomeRunning
          else {
            println(s"Becoming poll. ${fullState.values.map(_.taskStatus.map(_.getState.getValueDescriptor))}")
            poll
          }

        case _ =>
          if (quorum)
            becomeRunning
          else
            poll
      }
    }

    def handleEvent(event: Event, state: NodeState, fullState: Map[String, NodeState]): Directive = {
      event match {
        case Timer(_) =>
          stay
        case MatchedOffer(pendingOffer, _) =>
          if(!pendingOffer.offer.resources.exists(_.hasReservation))
            offerResponse(pendingOffer, Nil)
          else {
            lazy val peers = state.peers(fullState.values)
              (state.role, state.pState.goal) match {
              case (NodeRole.Monitor, Some(desiredState @ (RunState.Running | RunState.Paused))) =>
                persist(
                  state.pState.copy(
                    location = Some(configTemplates.deriveLocation(pendingOffer.offer)),
                    lastLaunched = Some(desiredState))).
                  andAlso(
                    offerResponse(
                      pendingOffer,
                      launchMonCommand(
                        isLeader = peers.forall(_.pState.goal.isEmpty),
                        pendingOffer,
                        state,
                        fullState,
                        configTemplates.inferPort(pendingOffer.offer.resources),
                        desiredState)))
              case _ =>
                ???
                // hold(pendingOffer, None)
            }
          }

        case n: NodeUpdated if state.taskStatus.isEmpty =>
          stay
        case n: NodeUpdated =>
          val status = state.taskStatus.get
          status.getState match {
            case Protos.TaskState.TASK_RUNNING =>
              stay
            case _ =>
              stay
          }
      }
    }
  }

  def launchMonCommand(
    isLeader: Boolean, pendingOffer: PendingOffer, state: NodeState, fullState: Map[String, NodeState], port: Int,
    runState: RunState.EnumVal):
      List[Protos.Offer.Operation] = {
    val mons = fullState.values.filter(_.role == NodeRole.Monitor)
    def knownMonLocations =
      mons.exists { _.pState.location.nonEmpty }

    // We launch!
    val container = Protos.ContainerInfo.newBuilder.
      setType(Protos.ContainerInfo.Type.DOCKER).
      setDocker(
        Protos.ContainerInfo.DockerInfo.newBuilder.
          setImage("ceph/daemon:tag-build-master-jewel-ubuntu-14.04").
          setNetwork(Protos.ContainerInfo.DockerInfo.Network.HOST).
          setForcePullImage(true)
      ).
      addVolumes(
        newVolume(
          containerPath = "/etc/ceph",
          hostPath = "state/etc")).
      addVolumes(
        newVolume(
          containerPath = "/var/lib/ceph",
          hostPath = "state/var"))

    val pullMonMapCommand = if (isLeader) {
      ""
    } else {
      """if [ ! -f /etc/ceph/monmap-ceph ]; then
        |  echo "Pulling monitor map"; ceph mon getmap -o /etc/ceph/monmap-ceph
        |fi
        |""".stripMargin
    }

    val templatesTgz = configTemplates.tgz(
      secrets = secrets,
      monIps = mons.flatMap(_.pState.location),
      leaderOffer = if (isLeader) Some(pendingOffer.offer) else None,
      cephSettings = deploymentConfig().settings)

    val taskInfo = Protos.TaskInfo.newBuilder.
      setTaskId(newTaskId(state.taskId)).
      setLabels(newLabels(
        Constants.FrameworkIdLabel -> frameworkId().getValue,
        Constants.HostnameLabel -> pendingOffer.offer.getHostname,
        Constants.PortLabel -> port.toString)).
      setName("ceph-monitor").
      setContainer(container).
      setSlaveId(pendingOffer.offer.getSlaveId).
      addAllResources(pendingOffer.offer.getResourcesList).
      setCommand(
        Protos.CommandInfo.newBuilder.
          setShell(true).
          setEnvironment(
            newEnvironment(
              "CEPH_PUBLIC_NETWORK" -> appConfig.publicNetwork,
              "CEPH_CONFIG_TGZ" -> Base64.getEncoder.encodeToString(templatesTgz))).
          setValue(s"""
            |echo "$$CEPH_CONFIG_TGZ" | base64 -d | tar xz -C / --overwrite
            |sed -i "s/:6789/:${port}/g" /entrypoint.sh config.static.sh
            |export MON_IP=$$(hostname -i | cut -f 1 -d ' ')
            |echo MON_IP = $$MON_IP
            |${pullMonMapCommand}
            |/entrypoint.sh mon
            |""".stripMargin))

    List(
      newOfferOperation(
        newLaunchOperation(Seq(taskInfo.build))))
  }

  def defaultBehaviorFactory = InitializeLogic
}
