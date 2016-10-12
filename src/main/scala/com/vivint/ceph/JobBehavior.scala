package com.vivint.ceph

import java.util.UUID
import org.apache.mesos.Protos
import com.vivint.ceph.model.{ Location, PartialLocation, TaskState }
import com.vivint.ceph.model.{ JobRole, RunState, CephConfig, Job, ServiceLocation }
import JobFSM._
import Behavior._
import scala.collection.immutable.NumericRange
import scala.concurrent.duration._
import scala.collection.breakOut
import scala.collection.JavaConverters._
import model.ClusterSecrets
import scaldi.Injector
import scaldi.Injectable._
import mesosphere.mesos.protos.Resource.PORTS
import ProtoHelpers._
import java.util.Base64
import Directives._

class JobBehavior(
  secrets: ClusterSecrets,
  frameworkId: () => Protos.FrameworkID,
  deploymentConfig: () => CephConfig)(implicit injector: Injector)
    extends BehaviorSet {

  private def getMonLocations(fullState: Map[UUID, Job]): Set[ServiceLocation] =
    fullState.values.
      filter(_.role == JobRole.Monitor).
      flatMap{_.pState.serviceLocation}(breakOut)

  val resolver = inject[String => String]('ipResolver)
  val offerOperations = inject[OfferOperations]
  val configTemplates = inject[views.ConfigTemplates]
  val appConfig = inject[AppConfiguration]

  def decideWhatsNext(state: Job, fullState: Map[UUID, Job]): Directives.Directive = {
    import Directives._
    state.pState match {
      case pState if state.version != state.persistentVersion =>
        // try again; previous persistence must have timed out.
        Persist(pState).
          withTransition(
            WaitForSync(decideWhatsNext))
      case pState =>
        if (pState.reservationConfirmed)
          Transition(Running)
        else if (pState.slaveId.nonEmpty)
          Transition(WaitForReservation)
        else
          Transition(MatchForReservation)
    }
  }

  case object InitializeResident extends Behavior {
    override def preStart(state: Job, fullState: Map[UUID, Job]): Directive = {
      decideWhatsNext(state: Job, fullState: Map[UUID, Job]): Directive
    }

    def handleEvent(event: Event, state: Job, fullState: Map[UUID, Job]): Directive =
      throw new IllegalStateException("handleEvent called on InitializeResident")
  }

  case class WaitForSync(nextBehavior: DecideFunction) extends Behavior {
    override def preStart(state: Job, fullState: Map[UUID, Job]): Directive = {
      SetBehaviorTimer("timeout", 30.seconds)
    }

    def handleEvent(event: Event, state: Job, fullState: Map[UUID, Job]): Directive = {
      event match {
        case Timer("timeout") =>
          nextBehavior(state, fullState)
        case Timer(_) =>
          Stay
        case JobUpdated(prior) =>
          if (state.persistentVersion < state.version)
            Stay
          else
            nextBehavior(state, fullState)
        case MatchedOffer(offer, matchResult) =>
          Hold(offer, matchResult)
      }
    }
  }

  case object MatchForReservation extends Behavior {
    override def preStart(state: Job, fullState: Map[UUID, Job]): Directive = {
      WantOffers
    }

    def handleEvent(event: Event, state: Job, fullState: Map[UUID, Job]): Directive = {
      event match {
        case Timer(_) =>
          Stay
        case JobUpdated(_) =>
          Stay
        case MatchedOffer(pendingOffer, matchResult) =>
          val resources = pendingOffer.offer.resources
          if (resources.forall { r => r.hasReservation }) {

            val newState = state.pState.copy(
              reservationConfirmed = true,
              slaveId = Some(pendingOffer.slaveId))
            Persist(newState) andAlso Hold(pendingOffer, matchResult) withTransition (Running)
          } else {
            matchResult match {
              case Some(result) =>
                OfferResponse(
                  pendingOffer,
                  offerOperations.reserveAndCreateVolumes(frameworkId(), state.id, result)).
                  andAlso(
                    Persist(
                      state.pState.copy(slaveId = Some(pendingOffer.slaveId)))).
                  withTransition(WaitForReservation)
              case None =>
                OfferResponse(pendingOffer, Nil) andAlso WantOffers
            }
          }
      }
    }
  }

  case object WaitForReservation extends Behavior {
    override def preStart(state: Job, fullState: Map[UUID, Job]): Directive = {
      SetBehaviorTimer("timeout", 30.seconds)
    }

    def handleEvent(event: Event, state: Job, fullState: Map[UUID, Job]): Directive = {
      event match {
        case Timer("timeout") =>
          Transition(MatchForReservation)
        case Timer(_) =>
          Stay
        case MatchedOffer(pendingOffer, matchResult) =>
          if (pendingOffer.offer.resources.exists(_.hasReservation)) {
            val newState = state.pState.copy(
              reservationConfirmed = true)
            Persist(newState).
              andAlso(Hold(pendingOffer, matchResult)).
              withTransition(WaitForSync(decideWhatsNext))
          } else {
            Hold(pendingOffer, matchResult).withTransition(MatchForReservation)
          }
        case JobUpdated(_) =>
          Stay
      }
    }
  }

  case class Sleep(duration: FiniteDuration, andThen: DecideFunction) extends Behavior {
    override def preStart(state: Job, fullState: Map[UUID, Job]): Directive = {
      SetBehaviorTimer("wakeup", duration)
    }

    def handleEvent(event: Event, state: Job, fullState: Map[UUID, Job]): Directive = {
      event match {
        case Timer("wakeup") =>
          andThen(state, fullState)
        case Timer(_) | JobUpdated(_) =>
          Stay
        case MatchedOffer(offer, matchResult) =>
          Hold(offer, matchResult)
      }
    }
  }


  val holdingOffers: PartialFunction[JobFSM.Event, Directive] = {
    case MatchedOffer(offer, matchResult) =>
      Hold(offer, matchResult)
  }

  case class Killing(duration: FiniteDuration, andThen: TransitionFunction) extends Behavior {
    override def preStart(state: Job, fullState: Map[UUID, Job]): Directive = {
      if (state.runningState.isEmpty)
        throw new IllegalStateException("can't kill a non-running task")

      SetBehaviorTimer("timeout", duration).andAlso(KillTask)
    }

    def handleEvent(event: Event, state: Job, fullState: Map[UUID, Job]): Directive = {
      handleWith(event) {
        holdingOffers orElse {
          case Timer("timeout") =>
            preStart(state, fullState)
          case JobUpdated(_) if (state.runningState.isEmpty) =>
            Transition(andThen(state, fullState))
        }
      }
    }
  }

  case class WaitForGoal(andThen: TransitionFunction) extends Behavior {
    def handleEvent(event: Event, state: Job, fullState: Map[UUID, Job]): Directive = {
      handleWith(event) {
        holdingOffers orElse {
          case JobUpdated(_) if (state.goal.nonEmpty) =>
            Transition(andThen(state, fullState))
        }
      }
    }
  }


  case object MatchAndLaunchEphemeral extends Behavior {
    override def preStart(state: Job, fullState: Map[UUID, Job]): Directive = {
      if (state.goal.isEmpty)
        Transition(WaitForGoal{ (_,_) => MatchAndLaunchEphemeral})
      else if (state.taskStatus.nonEmpty)
        Transition(EphemeralRunning)
      else
        WantOffers
    }

    def handleEvent(event: Event, state: Job, fullState: Map[UUID, Job]): Directive = event match {
      case MatchedOffer(pendingOffer, matchResult) =>
        state.role match {
          case JobRole.RGW =>
            val port =
              deploymentConfig().deployment.rgw.port orElse inferPort(pendingOffer.offer.resources)
            val location = PartialLocation(None, port)
            val newTaskId = Job.makeTaskId(state.role, state.cluster)
            Persist(
              state.pState.copy(
                slaveId = Some(pendingOffer.offer.getSlaveId.getValue),
                taskId = Some(newTaskId),
                location = location
              )).
              andAlso(
                OfferResponse(
                  pendingOffer,
                  launchRGWCommand(
                    taskId = newTaskId,
                    pendingOffer.offer,
                    state,
                    location = location,
                    monLocations = getMonLocations(fullState),
                    port = port.getOrElse(80)))).
              withTransition(EphemeralRunning)
          case _ =>
            ???
        }
      case _  =>
        Stay
    }
  }

  case object EphemeralRunning extends Behavior {
    override def preStart(state: Job, fullState: Map[UUID, Job]): Directive = {
      if (state.taskId.isEmpty)
        throw new IllegalStateException(s"Can't be EphemeralRunning without a taskId")
      if (state.slaveId.isEmpty)
        throw new IllegalStateException(s"Can't be EphemeralRunning without a slaveId")

      SetBehaviorTimer("timeout", 60.seconds) andAlso decideWhatsNext(state)
    }

    def relaunch(state:Job): Directive =
      Persist(state.pState.copy(taskId = None, slaveId = None, location = Location.empty)).
        withTransition(MatchAndLaunchEphemeral)

    def decideWhatsNext(state: Job): Directive = {
      state.taskState match {
        case Some(_: TaskState.Terminal) =>
          relaunch(state)
        case other =>
          Stay
      }
    }

    def handleEvent(event: Event, state: Job, fullState: Map[UUID, Job]): Directive = event match {
      case JobUpdated(_) =>
        decideWhatsNext(state)
      case Timer("timeout") =>
        if (state.taskState.isEmpty) {
          // task failed to launch. Try again.
          relaunch(state)
        } else {
          Stay
        }
      case Timer(_) =>
        Stay
      case MatchedOffer(offer, _) =>
        OfferResponse(offer, Nil)
    }
  }

  case object Running extends Behavior {
    def reservationConfirmed(state:Job) =
      state.pState.reservationConfirmed

    def getMonitors(fullState: Map[UUID, Job]) =
      fullState.values.filter(_.role == JobRole.Monitor).toList

    override def preStart(state: Job, fullState: Map[UUID, Job]): Directive = {
      if(! state.reservationConfirmed)
        throw new IllegalStateException("Can't go to running state without a confirmed reservation")
      Revive andAlso nextRunAction(state, fullState)
    }

    def nextRunAction(state: Job, fullState: Map[UUID, Job]): Directive = {
      (state.runningState, state.goal) match {
        case (_, None) =>
          Transition(WaitForGoal( { (_,_) => Running }))

        case (Some(running), Some(goal)) if running != goal =>
          // current running state does not match goal
          Transition(Killing(70.seconds, { (_, _) => Running }))

        case (_, Some(_)) =>
          // if we already have a goal then proceed
          Stay
      }
    }

    def handleEvent(event: Event, state: Job, fullState: Map[UUID, Job]): Directive = {
      event match {
        case Timer(_) =>
          Stay
        case MatchedOffer(pendingOffer, _) =>
          if (state.runningState.nonEmpty)
            /* We shouldn't get the persistent offer for a task unless if the task is dead, so we shouldn't have to
             * worry about this. Unless if two persistent offers were made for a task, on the same slave... which semes
             * very unlikely */
            throw new IllegalStateException("As assumption made by the framework author was wrong")

          if(!pendingOffer.offer.resources.exists(_.hasReservation))
            OfferResponse(pendingOffer, Nil)
          else {
            lazy val peers = state.peers(fullState.values)
            lazy val monLocations = getMonLocations(fullState)
            lazy val taskLocation = deriveLocation(pendingOffer.offer)
            val newTaskId = Job.makeTaskId(state.role, state.cluster)
            val persistDirective = Persist(
              state.pState.copy(
                location = taskLocation,
                taskId = Some(newTaskId),
                lastLaunched = state.goal))

            (state.role, state.pState.goal) match {
              case (JobRole.Monitor, Some(desiredState @ (RunState.Running | RunState.Paused))) =>
                persistDirective.
                  andAlso(
                    OfferResponse(
                      pendingOffer,
                      launchMonCommand(
                        taskId = newTaskId,
                        isLeader = peers.forall(_.pState.goal.isEmpty),
                        pendingOffer.offer,
                        state,
                        taskLocation = taskLocation,
                        desiredState,
                        monLocations = monLocations
                      )))
              case (JobRole.OSD, Some(desiredState @ (RunState.Running | RunState.Paused))) =>
                persistDirective.
                  andAlso(
                    OfferResponse(
                      pendingOffer,
                      launchOSDCommand(
                        taskId = newTaskId,
                        pendingOffer.offer,
                        state,
                        taskLocation = taskLocation,
                        desiredState,
                        monLocations = monLocations)))

              case _ =>
                ???
            }
          }
        case JobUpdated(prior) =>
          // if the goal has changed then we need to revaluate our next run state
          val next = nextRunAction(state, fullState)
          if (prior.runningState.nonEmpty && state.runningState.isEmpty) /* oops we lost it! */ {
            Revive andAlso next
          } else {
            next
          }
      }
    }
  }

  private def inferPortRange(resources: Iterable[Protos.Resource], default: NumericRange.Inclusive[Long] = 6800L to 7300L):
      NumericRange.Inclusive[Long] =
    resources.
      toStream.
      filter(_.getName == PORTS).
      flatMap(_.ranges).
      headOption.
      getOrElse(default)

  private def inferPort(resources: Iterable[Protos.Resource]): Option[Int] =
    resources.
      toStream.
      filter(_.getName == PORTS).
      flatMap(_.ranges).
      headOption.
      map(_.min.toInt)

  private def deriveLocation(offer: Protos.Offer, defaultPort: Int = 6789): ServiceLocation = {
    val ip = resolver(offer.getHostname)

    val port = inferPort(offer.resources)

    ServiceLocation(
      offer.hostname.get,
      ip,
      port.getOrElse(defaultPort))
  }

  private def launchMonCommand(
    taskId: String, isLeader: Boolean, offer: Protos.Offer, job: Job, taskLocation: ServiceLocation,
    runState: RunState.EnumVal, monLocations: Set[ServiceLocation]):
      List[Protos.Offer.Operation] = {

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
      monitors = (monLocations + taskLocation),
      cephSettings = deploymentConfig().settings)

    val taskInfo = launchCephCommand(
      taskId = taskId,
      jobId = job.id,
      role = job.role,
      offer = offer,
      location = taskLocation,
      templatesTgz = templatesTgz,
      vars = Seq("MON_IP" -> taskLocation.ip, "MON_NAME" -> taskLocation.hostname),
      command =
        runState match {
          case RunState.Running =>
            s"""
            |sed -i "s/:6789/:${taskLocation.port}/g" /entrypoint.sh config.static.sh
            |${pullMonMapCommand}
            |/entrypoint.sh mon
            |""".stripMargin
          case RunState.Paused =>
            s"""
            |sleep 86400
            |""".stripMargin
        }
    )

    List(
      newOfferOperation(
        newLaunchOperation(Seq(taskInfo.build))))
  }

  private def launchOSDCommand(
    taskId: String, offer: Protos.Offer, job: Job, taskLocation: ServiceLocation,
    runState: RunState.EnumVal, monLocations: Set[ServiceLocation]):
      List[Protos.Offer.Operation] = {

    val pullMonMapCommand = {
      """if [ ! -f /etc/ceph/monmap-ceph ]; then
        |  echo "Pulling monitor map"; ceph mon getmap -o /etc/ceph/monmap-ceph
        |fi
        |""".stripMargin
    }

    val templatesTgz = configTemplates.tgz(
      secrets = secrets,
      monitors = monLocations,
      cephSettings = deploymentConfig().settings,
      osdPort = Some(inferPortRange(offer.resources.toList)))

    val taskInfo = launchCephCommand(
      taskId = taskId,
      jobId = job.id,
      role = job.role,
      offer = offer,
      location = taskLocation,
      templatesTgz = templatesTgz,
      command =
        runState match {
          case RunState.Running =>
            s"""
            |if [ "$$(df -T /var/lib/ceph | tail -n 1 | awk '{print $$2}')" != "xfs" ]; then
            |  echo "Cowardly refusing to OSD start on non-xfs volume."
            |  echo "Cowardly refusing to OSD start on non-xfs volume." 1>&2
            |  echo "Please see http://docs.ceph.com/docs/jewel/rados/configuration/filesystem-recommendations/#not-recommended for more information"
            |  sleep 60
            |  exit
            |fi
            |set -x -e
            |echo "Pulling monitor map"
            |ceph mon getmap -o /etc/ceph/monmap-ceph
            |
            |if [ ! -f /etc/ceph/my_osd_id ]; then
            |  ceph osd create > /etc/ceph/my_osd_id
            |fi
            |OSD_ID=$$(cat /etc/ceph/my_osd_id)
            |mkdir -p /var/lib/ceph/osd/ceph-$${OSD_ID}
            |chown ceph:ceph /var/lib/ceph/osd/ceph-$${OSD_ID}
            |
            |/entrypoint.sh osd_directory
            |""".stripMargin
          case RunState.Paused =>
            s"""
            |sleep 86400
            |""".stripMargin
        }
    )

    List(
      newOfferOperation(
        newLaunchOperation(Seq(taskInfo.build))))
  }

  private def launchRGWCommand(
    taskId: String, offer: Protos.Offer, job: Job, location: Location,
    monLocations: Set[ServiceLocation], port: Int):
      List[Protos.Offer.Operation] = {
    val pullMonMapCommand = {
      """if [ ! -f /etc/ceph/monmap-ceph ]; then
        |  echo "Pulling monitor map"; ceph mon getmap -o /etc/ceph/monmap-ceph
        |fi
        |""".stripMargin
    }

    val cephConfig = deploymentConfig()
    val templatesTgz = configTemplates.tgz(
      secrets = secrets,
      monitors = monLocations,
      cephSettings = cephConfig.settings)

    val taskInfo = launchCephCommand(
      taskId = taskId,
      jobId = job.id,
      role = job.role,
      offer = offer,
      location = location,
      templatesTgz = templatesTgz,
      dockerArgs = cephConfig.deployment.rgw.docker_args,
      command =
            s"""
            |set -x -e
            |echo "Pulling monitor map"
            |ceph mon getmap -o /etc/ceph/monmap-ceph
            |
            |RGW_CIVETWEB_PORT=${port} /entrypoint.sh rgw
            |""".stripMargin
    )

    List(
      newOfferOperation(
        newLaunchOperation(Seq(taskInfo.build))))
  }


  private def launchCephCommand(taskId: String, jobId: UUID, role: JobRole.EnumVal, command: String,
    offer: Protos.Offer, location: Location, vars: Seq[(String, String)] = Nil, templatesTgz: Array[Byte],
    dockerArgs: Map[String, String] = Map.empty) = {
    // We launch!

    val dockerParameters = (location.hostnameOpt.map("hostname" -> _) ++ dockerArgs).map((newParameter(_,_)).tupled)
    val container = Protos.ContainerInfo.newBuilder.
      setType(Protos.ContainerInfo.Type.DOCKER).
      setDocker(
        Protos.ContainerInfo.DockerInfo.newBuilder.
          setImage("ceph/daemon:tag-build-master-jewel-ubuntu-14.04").
          setNetwork(Protos.ContainerInfo.DockerInfo.Network.HOST).
          setForcePullImage(true).
          addAllParameters(dockerParameters.asJava)
      ).
      addVolumes(
        newVolume(
          containerPath = "/etc/ceph",
          hostPath = "state/etc")).
      addVolumes(
        newVolume(
          containerPath = "/var/lib/ceph",
          hostPath = "state/var"))

    val env = newEnvironment(
      (Seq(
        "MESOS_TASK_ID" -> taskId,
        "CEPH_ROLE" -> role.name,
        "CEPH_PUBLIC_NETWORK" -> appConfig.publicNetwork,
        "CEPH_CONFIG_TGZ" -> Base64.getEncoder.encodeToString(templatesTgz))
        ++
        vars) : _*)


    val discovery = Protos.DiscoveryInfo.newBuilder.
      setVisibility(Protos.DiscoveryInfo.Visibility.FRAMEWORK).
      setName(role.toString)

    location.portOpt.foreach { port =>
      discovery.
        setPorts(Protos.Ports.newBuilder.addPorts(
          Protos.Port.newBuilder.
            setName(role.toString).
            setNumber(port).
            setProtocol("TCP")))
    }

    val labels = Protos.Labels.newBuilder
    labels.addLabels(newLabel(Constants.JobIdLabel, jobId.toString))
    labels.addLabels(newLabel(Constants.FrameworkIdLabel, frameworkId().getValue))
    location.hostnameOpt.foreach { hostname =>
      labels.addLabels(newLabel(Constants.HostnameLabel, hostname))
    }
    location.portOpt.foreach { port =>
      labels.addLabels(newLabel(Constants.PortLabel, port.toString))
    }

    val taskInfo = Protos.TaskInfo.newBuilder.
      setTaskId(newTaskId(taskId)).
      setLabels(labels).
      setName(s"ceph-${role}").
      setDiscovery(discovery).
      setContainer(container).
      setSlaveId(offer.getSlaveId).
      addAllResources(offer.getResourcesList).
      setCommand(
        Protos.CommandInfo.newBuilder.
          setShell(true).
          setEnvironment(env).
          setValue(s"""
            |echo "$$CEPH_CONFIG_TGZ" | base64 -d | tar xz -C / --overwrite
            |${command}
            |""".stripMargin))
    taskInfo
  }

  def defaultBehavior(role: JobRole.EnumVal) = role match {
    case JobRole.Monitor | JobRole.OSD => InitializeResident
    case JobRole.RGW => MatchAndLaunchEphemeral
  }
}
