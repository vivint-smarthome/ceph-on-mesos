package org.vivint.ceph

import akka.actor.{ ActorContext, Cancellable }
import java.util.concurrent.atomic.AtomicInteger
import mesosphere.mesos.protos.TaskStatus
import org.apache.mesos.Protos
import org.vivint.ceph.model.{ CephNode, CephConfig, NodeState, ServiceLocation }
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
  frameworkId: () => String,
  deploymentConfig: () => CephConfig)(implicit injector: Injector)
    extends BehaviorSet {

  val offerOperations = inject[OfferOperations]
  val configTemplates = inject[views.ConfigTemplates]

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
        if (pState.slaveId.nonEmpty)
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
            persist(newState).
              withTransition(WaitForSync(decideWhatsNext))
          } else {
            hold(pendingOffer, matchResult).withTransition(Matching)
          }
        case NodeUpdated(_) =>
          stay
      }
    }
  }

  case class Running(taskId: String, actorContext: ActorContext) extends Behavior {
    def reservationConfirmed(state:NodeState) =
      state.inferPersistedState.reservationConfirmed

    override def preStart(state: NodeState, fullState: Map[String, NodeState]): Directive = {
      if(! reservationConfirmed(state))
        throw new IllegalStateException("Can't go to running state without a confirmed reservation")
      stay
    }

    private def inferLocationFromStatus(ts: Protos.TaskStatus) = {
      import scala.collection.JavaConversions._
      val ip = ts.getContainerStatus.getNetworkInfosList.
        toStream.
        flatMap { networkInfo =>
          networkInfo.getIpAddressesList.toStream
        }.
        headOption.
        map { ipAddress =>
          ipAddress.getIpAddress
        }
      val port = ts.getLabels.get(Constants.PortLabel).get.toInt
      val hostname = ts.getLabels.get(Constants.HostnameLabel).get

      ServiceLocation(
        slaveId = ts.getSlaveId.getValue,
        port = port,
        hostname = hostname,
        ip = ip.get)
    }

    def inferPort(resources: Iterable[Protos.Resource]): Int =
      resources.
        filter(_.getName == PORTS).
        flatMap { p =>
          p.ranges
        }.
        head.
        min.
        toInt

    def handleEvent(event: Event, state: NodeState, fullState: Map[String, NodeState]): Directive = {
      event match {
        case Timer(_) =>
          stay
        case MatchedOffer(pendingOffer, _) =>
          if(!pendingOffer.offer.resources.exists(_.hasReservation))
            offerResponse(pendingOffer, Nil)
          else
            offerResponse(pendingOffer,
              determineLaunchCommand(pendingOffer, state, fullState, inferPort(pendingOffer.offer.resources)))
        case n: NodeUpdated if state.taskStatus.isEmpty =>
          stay
        case n: NodeUpdated =>
          val status = state.taskStatus.get
          status.getState match {
            case Protos.TaskState.TASK_RUNNING =>
              persist(
                state.inferPersistedState.copy(
                  location = Some(inferLocationFromStatus(status))))
            case _ =>
              stay
          }
      }
    }
  }

  def determineLaunchCommand(
    pendingOffer: PendingOffer, state: NodeState, fullState: Map[String, NodeState], port: Int):
      List[Protos.Offer.Operation] = {
    val mons = fullState.values.filter(_.role == "mon")
    def knownMonLocations =
      mons.exists { _.inferPersistedState.location.nonEmpty }
    def isLeader =
      (state.id.toString == mons.map(_.id.toString).min)

    if (knownMonLocations || isLeader) {
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

      // setContainer(x$1: ContainerInfo)

      val templatesTgz = configTemplates.tgz(
        secrets = secrets,
        monIps = mons.flatMap(_.inferPersistedState.location),
        leaderOffer = if (isLeader) Some(pendingOffer.offer) else None,
        cephSettings = deploymentConfig().settings)

      val taskInfo = Protos.TaskInfo.newBuilder.
        setTaskId(newTaskId(state.taskId)).
        setLabels(newLabels(
          Constants.FrameworkIdLabel -> frameworkId(),
          Constants.HostnameLabel -> pendingOffer.offer.getHostname,
          Constants.PortLabel -> port.toString)).
        setName("le ceph").
        setSlaveId(pendingOffer.offer.getSlaveId).
        addAllResources(pendingOffer.offer.getResourcesList).
        setCommand(
          Protos.CommandInfo.newBuilder.
            setShell(true).
            setEnvironment(
              newEnvironment(
                "CEPH_CONFIG_TGZ" -> Base64.getEncoder.encodeToString(templatesTgz))).
            setValue(s"""
            |echo "$$CEPH_CONFIG_TGZ" | base64 -D | tar xz -C /
            |sed -i "s/:6789/:${port}/g" /entrypoint.sh
            |/entrypoint.sh mon
            |""".stripMargin))

      List(
        Protos.Offer.Operation.newBuilder.
          setLaunch(
            Protos.Offer.Operation.Launch.newBuilder.
              addTaskInfos(taskInfo)).
          build)
    } else {
      // We wait
      Nil
    }
  }

  def defaultBehaviorFactory = InitializeLogic
}
