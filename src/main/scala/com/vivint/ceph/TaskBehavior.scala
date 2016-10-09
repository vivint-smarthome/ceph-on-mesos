package com.vivint.ceph

import akka.actor.{ ActorContext, Cancellable }
import java.util.concurrent.atomic.AtomicInteger
import mesosphere.mesos.protos.TaskStatus
import org.apache.mesos.Protos
import com.vivint.ceph.model.{ TaskRole, RunState, PersistentState, CephConfig, Task, ServiceLocation }
import TaskFSM._
import Behavior._
import scala.annotation.tailrec
import scala.collection.immutable.NumericRange
import scala.concurrent.duration._
import scala.collection.breakOut
import model.ClusterSecrets
import scaldi.Injector
import scaldi.Injectable._
import mesosphere.mesos.protos.Resource.PORTS
import ProtoHelpers._
import java.util.Base64

class TaskBehavior(
  secrets: ClusterSecrets,
  frameworkId: () => Protos.FrameworkID,
  deploymentConfig: () => CephConfig)(implicit injector: Injector)
    extends BehaviorSet {

  val resolver = inject[String => String]('ipResolver)
  val offerOperations = inject[OfferOperations]
  val configTemplates = inject[views.ConfigTemplates]
  val appConfig = inject[AppConfiguration]

  def decideWhatsNext(state: Task, fullState: Map[String, Task]): Directive = {
    import Directives._
    state.persistentState match {
      case None =>
        persist(state.pState).
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

  case object InitializeLogic extends Behavior {
    override def preStart(state: Task, fullState: Map[String, Task]): Directive = {
      decideWhatsNext(state: Task, fullState: Map[String, Task]): Directive
    }

    def handleEvent(event: Event, state: Task, fullState: Map[String, Task]): Directive =
      throw new IllegalStateException("handleEvent called on InitializeLogic")
  }

  case class WaitForSync(nextBehavior: DecideFunction) extends Behavior {
    override def preStart(state: Task, fullState: Map[String, Task]): Directive = {
      setBehaviorTimer("timeout", 30.seconds)
    }

    def handleEvent(event: Event, state: Task, fullState: Map[String, Task]): Directive = {
      event match {
        case Timer("timeout") =>
          nextBehavior(state, fullState)
        case Timer(_) =>
          stay
        case TaskUpdated(prior) =>
          if (state.persistentVersion < state.version)
            stay
          else
            nextBehavior(state, fullState)
        case MatchedOffer(offer, matchResult) =>
          hold(offer, matchResult)
      }
    }
  }

  case object Matching extends Behavior {
    override def preStart(state: Task, fullState: Map[String, Task]): Directive = {
      wantOffers
    }

    def handleEvent(event: Event, state: Task, fullState: Map[String, Task]): Directive = {
      event match {
        case Timer(_) =>
          stay
        case TaskUpdated(_) =>
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
                  offerOperations.reserveAndCreateVolumes(frameworkId(), state.taskId, result)).
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

  case object WaitForReservation extends Behavior {
    override def preStart(state: Task, fullState: Map[String, Task]): Directive = {
      setBehaviorTimer("timeout", 30.seconds)
    }

    def handleEvent(event: Event, state: Task, fullState: Map[String, Task]): Directive = {
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
              andAlso(hold(pendingOffer, matchResult)).
              withTransition(WaitForSync(decideWhatsNext))
          } else {
            hold(pendingOffer, matchResult).withTransition(Matching)
          }
        case TaskUpdated(_) =>
          stay
      }
    }
  }

  case class Sleep(duration: FiniteDuration, andThen: DecideFunction) extends Behavior {
    override def preStart(state: Task, fullState: Map[String, Task]): Directive = {
      setBehaviorTimer("wakeup", duration)
    }

    def handleEvent(event: Event, state: Task, fullState: Map[String, Task]): Directive = {
      event match {
        case Timer("wakeup") =>
          andThen(state, fullState)
        case Timer(_) | TaskUpdated(_) =>
          stay
        case MatchedOffer(offer, matchResult) =>
          hold(offer, matchResult)
      }
    }
  }

  case class KillTask(duration: FiniteDuration, andThen: TransitionFunction) extends Behavior {
    override def preStart(state: Task, fullState: Map[String, Task]): Directive = {
      if (state.runningState.isEmpty)
        throw new IllegalStateException("can't kill a non-running task")

      setBehaviorTimer("timeout", duration).andAlso(killTask)
    }

    def handleEvent(event: Event, state: Task, fullState: Map[String, Task]): Directive = {
      event match {
        case Timer("timeout") =>
          preStart(state, fullState)
        case Timer(_) =>
          stay
        case TaskUpdated(_) =>
          if (state.runningState.isEmpty)
            transition(andThen(state, fullState))
          else
            stay
        case MatchedOffer(offer, matchResult) =>
          hold(offer, matchResult)
      }
    }
  }

  case object Running extends Behavior {
    def reservationConfirmed(state:Task) =
      state.pState.reservationConfirmed

    def getMonitors(fullState: Map[String, Task]) =
      fullState.values.filter(_.role == TaskRole.Monitor).toList

    override def preStart(state: Task, fullState: Map[String, Task]): Directive = {
      if(! reservationConfirmed(state))
        throw new IllegalStateException("Can't go to running state without a confirmed reservation")
      nextRunAction(state, fullState)
    }

    def nextRunAction(state: Task, fullState: Map[String, Task]): Directive = {
      (state.lastLaunched, state.runningState, state.goal) match {
        case (None, None, None) =>
          // This is our first launch. See if it's okay to initialize. This is where we implement the behavior for tasks
          // to wait for monitors to run
          /* TODO - move this logic to orchestrator!!! */
          val monitors = getMonitors(fullState)
          lazy val runningMonitors = monitors.filter(_.runningState == Some(RunState.Running)).toList
          lazy val quorumMonitorsAreRunning =
            runningMonitors.length > (monitors.length / 2) // NOTE this always fails for mon count [0, 1]
          def becomeRunning =
            persist(state.pState.copy(goal = Some(RunState.Running)))
          def poll =
            transition(Sleep(5.seconds, { (_, _) => transition(Running) }))

          state.role match {
            case TaskRole.Monitor if (runningMonitors.nonEmpty || (monitors.forall(_.pState.goal.isEmpty))) =>
              // am I the first or are others running?
              becomeRunning
            case _ if (quorumMonitorsAreRunning) =>
              becomeRunning
            case _ =>
              poll
          }

        case (_, Some(running), Some(goal)) if running != goal =>
          // current running state does not match goal
          transition(KillTask(70.seconds, { (_, _) => Running }))

        case (_, _, Some(_)) =>
          // if we already have a goal then proceed
          stay
        case (Some(launched), _, None) =>
          val jsonRepresentation = model.PlayJsonFormats.TaskWriter.writes(state)
          throw new IllegalStateException(
            s"Can't have launched something without a goal: launched = ${launched}; state = ${jsonRepresentation}")
      }
    }

    def handleEvent(event: Event, state: Task, fullState: Map[String, Task]): Directive = {
      event match {
        case Timer(_) =>
          stay
        case MatchedOffer(pendingOffer, _) =>
          println(s"Le offer!${pendingOffer.offer}")
          if (state.runningState.nonEmpty)
            /* We shouldn't get the persistent offer for a task unless if the task is dead, so we shouldn't have to
             * worry about this. Unless if two persistent offers were made for a task, on the same slave... which semes
             * very unlikely */
            throw new IllegalStateException("As assumption made by the framework author was wrong")

          if(!pendingOffer.offer.resources.exists(_.hasReservation))
            offerResponse(pendingOffer, Nil)
          else {
            lazy val peers = state.peers(fullState.values)
            lazy val monLocations: Set[ServiceLocation] = fullState.values.
              filter(_.role == TaskRole.Monitor).
              flatMap{_.pState.location}(breakOut)
            lazy val taskLocation = deriveLocation(pendingOffer.offer)

            (state.role, state.pState.goal) match {
              case (TaskRole.Monitor, Some(desiredState @ (RunState.Running | RunState.Paused))) =>

                persist(
                  state.pState.copy(
                    location = Some(taskLocation),
                    lastLaunched = Some(desiredState))).
                  andAlso(
                    offerResponse(
                      pendingOffer,
                      launchMonCommand(
                        isLeader = peers.forall(_.pState.goal.isEmpty),
                        pendingOffer.offer,
                        state,
                        taskLocation = taskLocation,
                        desiredState,
                        monLocations = monLocations
                      )))
              case (TaskRole.OSD, Some(desiredState @ (RunState.Running | RunState.Paused))) =>
                persist(
                  state.pState.copy(
                    location = Some(taskLocation),
                    lastLaunched = Some(desiredState))).
                  andAlso(
                    offerResponse(
                      pendingOffer,
                      launchOSDCommand(
                        pendingOffer.offer,
                        state,
                        taskLocation = taskLocation,
                        desiredState,
                        monLocations = monLocations)))

              case _ =>
                ???
                // hold(pendingOffer, None)
            }
          }
        case TaskUpdated(_) =>
          // if the goal has changed then we need to revaluate our next run state
          nextRunAction(state, fullState)
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

  private def inferPort(resources: Iterable[Protos.Resource], default: Int = 6789): Int =
    resources.
      toStream.
      filter(_.getName == PORTS).
      flatMap(_.ranges).
      headOption.
      map(_.min.toInt).
      getOrElse(default)

  private def deriveLocation(offer: Protos.Offer): ServiceLocation = {
    val ip = resolver(offer.getHostname)

    val port = inferPort(offer.resources)

    ServiceLocation(
      offer.hostname.get,
      ip,
      port)
  }

  private def launchMonCommand(
    isLeader: Boolean, offer: Protos.Offer, task: Task, taskLocation: ServiceLocation,
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
      taskId = task.taskId,
      role = task.role,
      offer = offer,
      taskLocation = taskLocation,
      templatesTgz = templatesTgz,
      command =
        runState match {
          case RunState.Running =>
            s"""
            |sed -i "s/:6789/:${taskLocation.port}/g" /entrypoint.sh config.static.sh
            |export MON_IP=$$(hostname -i | cut -f 1 -d ' ')
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
    offer: Protos.Offer, task: Task, taskLocation: ServiceLocation,
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
      taskId = task.taskId,
      role = task.role,
      offer = offer,
      taskLocation = taskLocation,
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

  private def launchCephCommand(taskId: String, role: TaskRole.EnumVal, command: String, offer: Protos.Offer,
    taskLocation: ServiceLocation, vars: Seq[(String, String)] = Nil, templatesTgz: Array[Byte]) = {
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

    val env = newEnvironment(
      (Seq(
        "MESOS_TASK_ID" -> taskId,
        "CEPH_ROLE" -> role.name,
        "CEPH_PUBLIC_NETWORK" -> appConfig.publicNetwork,
        "CEPH_CONFIG_TGZ" -> Base64.getEncoder.encodeToString(templatesTgz))
        ++
        vars) : _*)

    val taskInfo = Protos.TaskInfo.newBuilder.
      setTaskId(newTaskId(taskId)).
      setLabels(newLabels(
        Constants.FrameworkIdLabel -> frameworkId().getValue,
        Constants.HostnameLabel -> taskLocation.hostname,
        Constants.PortLabel -> taskLocation.port.toString)).
      setName(s"ceph-${role}").
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

  def defaultBehavior(role: TaskRole.EnumVal) = InitializeLogic
}
