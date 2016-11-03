package com.vivint.ceph

import java.util.UUID
import org.apache.mesos.Protos
import com.vivint.ceph.model.{ Location, PartialLocation }
import com.vivint.ceph.model.{ JobRole, RunState, CephConfig, Job, ServiceLocation }
import scala.collection.immutable.NumericRange
import scala.collection.breakOut
import scala.collection.JavaConverters._
import model.ClusterSecrets
import scaldi.Injector
import scaldi.Injectable._
import mesosphere.mesos.protos.Resource.PORTS
import ProtoHelpers._
import java.util.Base64

class JobBehavior(
  secrets: ClusterSecrets,
  frameworkId: Protos.FrameworkID,
  deploymentConfig: () => CephConfig)(implicit injector: Injector)
    extends BehaviorSet {

  private def getMonLocations(fullState: Map[UUID, Job]): Set[ServiceLocation] =
    fullState.values.
      filter(_.role == JobRole.Monitor).
      flatMap{_.pState.serviceLocation}(breakOut)

  val configTemplates = inject[views.ConfigTemplates]
  val appConfig = inject[AppConfiguration]

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
    val ip = offer.getUrl.getAddress.getIp

    val port = inferPort(offer.resources)

    ServiceLocation(
      offer.hostname.get,
      ip,
      port.getOrElse(defaultPort))
  }

  private def launchMonCommand(
    taskId: String, isLeader: Boolean, offer: Protos.Offer, job: Job, taskLocation: ServiceLocation,
    runState: RunState.EnumVal, monLocations: Set[ServiceLocation]): Protos.TaskInfo = {

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
      env = Seq("MON_IP" -> taskLocation.ip, "MON_NAME" -> taskLocation.hostname),
      command =
        runState match {
          case RunState.Running =>
            s"""
            |sed -i "s/:6789/:${taskLocation.port}/g" /entrypoint.sh config.static.sh
            |${pullMonMapCommand}
            |exec /entrypoint.sh mon
            |""".stripMargin
          case RunState.Paused =>
            s"""
            |sleep 86400
            |""".stripMargin
        }
    )

    taskInfo.build
  }

  private def launchOSDCommand(
    taskId: String, offer: Protos.Offer, job: Job, taskLocation: ServiceLocation,
    runState: RunState.EnumVal, monLocations: Set[ServiceLocation]):
      Protos.TaskInfo = {

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
            |FS_TYPE="$$(df -T /var/lib/ceph | tail -n 1 | awk '{print $$2}')"
            |if [ "$${FS_TYPE}" != "xfs" ] && [ "$${FS_TYPE}" != "btrfs" ]; then
            |  echo "Cowardly refusing to OSD start on non-xfs / non-btrfs volume."
            |  echo "Cowardly refusing to OSD start on non-xfs / non-btrfs volume." 1>&2
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
            |exec /entrypoint.sh osd_directory
            |""".stripMargin
          case RunState.Paused =>
            s"""
            |sleep 86400
            |""".stripMargin
        }
    )

    taskInfo.build
  }

  private def launchRGWCommand(
    taskId: String, offer: Protos.Offer, job: Job, location: Location,
    monLocations: Set[ServiceLocation], port: Int):
      Protos.TaskInfo = {
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
      env = Seq("RGW_CIVETWEB_PORT" -> port.toString),
      dockerArgs = cephConfig.deployment.rgw.docker_args,
      command =
            s"""
            |set -x -e
            |echo "Pulling monitor map"
            |ceph mon getmap -o /etc/ceph/monmap-ceph
            |
            |exec /entrypoint.sh rgw
            |""".stripMargin
    )

    taskInfo.build
  }


  private def launchCephCommand(taskId: String, jobId: UUID, role: JobRole.EnumVal, command: String,
    offer: Protos.Offer, location: Location, env: Seq[(String, String)] = Nil, templatesTgz: Array[Byte],
    dockerArgs: Map[String, String] = Map.empty) = {
    // We launch!

    val dockerParameters = (location.hostnameOpt.map("hostname" -> _) ++ dockerArgs).map((newParameter(_,_)).tupled)
    val container = Protos.ContainerInfo.newBuilder.
      setType(Protos.ContainerInfo.Type.DOCKER).
      setDocker(
        Protos.ContainerInfo.DockerInfo.newBuilder.
          setImage(deploymentConfig().deployment.docker_image).
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

    val environment = newEnvironment(
      (Seq(
        "MESOS_TASK_ID" -> taskId,
        "CEPH_ROLE" -> role.name,
        "CEPH_PUBLIC_NETWORK" -> appConfig.publicNetwork,
        "CEPH_CONFIG_TGZ" -> Base64.getEncoder.encodeToString(templatesTgz))
        ++
        env) : _*)


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
    labels.addLabels(newLabel(Constants.FrameworkIdLabel, frameworkId.getValue))
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
          setEnvironment(environment).
          setValue(s"""
            |echo "$$CEPH_CONFIG_TGZ" | base64 -d | tar xz -C / --overwrite
            |${command}
            |""".stripMargin))
    taskInfo
  }

  val mon = Behaviors(frameworkId, { (state, fullState, offer) =>
    val location = deriveLocation(offer)
    val task = launchMonCommand(
      taskId = Job.makeTaskId(state.role, state.cluster),
      isLeader = state.peers(fullState.values).forall(_.pState.goal.isEmpty),
      offer,
      state,
      taskLocation = location,
      state.goal.getOrElse(RunState.Paused),
      monLocations = getMonLocations(fullState))
    (location, task)
  })

  val osd = Behaviors(frameworkId, { (state, fullState, offer) =>
    val location = deriveLocation(offer)
    val task = launchOSDCommand(
      taskId = Job.makeTaskId(state.role, state.cluster),
      offer,
      state,
      taskLocation = location,
      state.goal.getOrElse(RunState.Paused),
      monLocations = getMonLocations(fullState))
    (location, task)
  })

  val rgw = Behaviors(frameworkId, { (state, fullState, offer) =>
    val port =
      deploymentConfig().deployment.rgw.port orElse inferPort(offer.resources)
    val location = PartialLocation(None, port)
    val task = launchRGWCommand(
      taskId = Job.makeTaskId(state.role, state.cluster),
      offer,
      state,
      location = location,
      monLocations = getMonLocations(fullState),
      port = port.getOrElse(80)
    )

    (location, task)
  })

  def defaultBehavior(role: JobRole.EnumVal) = role match {
    case JobRole.Monitor =>
      mon.InitializeResident

    case JobRole.OSD =>
      osd.InitializeResident

    case JobRole.RGW =>
      rgw.MatchAndLaunchEphemeral
  }
}
