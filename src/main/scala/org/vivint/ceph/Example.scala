// package org.vivint.ceph

// import java.util.{ Arrays, Collection }
// import org.apache.mesos.{ Protos, Scheduler }
// import org.apache.mesos.curator.CuratorStateStore
// import org.apache.mesos.offer.{ ResourceUtils, ValueUtils }
// import org.apache.mesos.protobuf.DefaultVolumeSpecification
// import org.apache.mesos.scheduler.{ DefaultScheduler, SchedulerDriverFactory, SchedulerUtils }
// import org.apache.mesos.specification.{ DefaultResourceSpecification, DefaultService, DefaultTaskTypeSpecification, ResourceSpecification, Service, ServiceSpecification, TaskTypeSpecification, VolumeSpecification }
// import org.slf4j.LoggerFactory
// import scaldi.Injector
// import scaldi.Injectable._
// //remove if not needed
// import scala.collection.JavaConversions._

// class Example {
//   private val log = LoggerFactory.getLogger(getClass)
//   private val SERVICE_NAME = "kafka"
//   private val BROKER_NAME = "broker"
//   private val BROKER_COUNT = Option(System.getenv("BROKER_COUNT")).map(_.toInt).getOrElse(1)
//   private val BROKER_CPU = Option(System.getenv("BROKER_CPU")).map(_.toDouble).getOrElse(0.05)
//   private val BROKER_MEM = Option(System.getenv("BROKER_MEM")).map(_.toDouble).getOrElse(256.0)
//   private val BROKER_DISK = Option(System.getenv("BROKER_DISK")).map(_.toDouble).getOrElse(16.0)
//   private val API_PORT = Option(System.getenv("PORT0")).map(_.toInt).getOrElse(30002)
//   private val BROKER_PORT = 30001L

//   def doTheThing(implicit inj: Injector): Unit = {
//     val cfg = inject[AppConfiguration]
//     log.info("Starting reference scheduler with args: {}", cfg)
//     new LeService(cfg).register(getServiceSpecification)
//   }

//   private def getServiceSpecification(): ServiceSpecification =
//     new ServiceSpecification() {
//       override def getName(): String = SERVICE_NAME
//       override def getTaskSpecifications(): java.util.List[TaskTypeSpecification] =
//         List(BrokerTaskTypeSpecification.getBrokerSpecification)
//     }

//   object BrokerTaskTypeSpecification {
//     private val ROLE = SchedulerUtils.nameToRole(SERVICE_NAME)
//     private val PRINCIPAL = SchedulerUtils.nameToPrincipal(SERVICE_NAME)
//     private val KAFKA_URI = "https://downloads.mesosphere.com/kafka/assets/kafka_2.11-0.10.0.0.tgz"
//     private val JAVA_URI = "https://downloads.mesosphere.com/kafka/assets/jre-8u91-linux-x64.tar.gz"
//     private val CONTAINER_PATH = "kafka_logs"

//     def getBrokerSpecification(): BrokerTaskTypeSpecification = {
//       new BrokerTaskTypeSpecification(
//         count = BROKER_COUNT,
//         name = BROKER_NAME,
//         command = null,
//         resources = getResources(BROKER_CPU, BROKER_MEM),
//         volumes = getVolumes(BROKER_DISK))
//     }

//     private def getResources(cpu: Double, mem: Double): Collection[ResourceSpecification] = {
//       Arrays.asList(
//         new DefaultResourceSpecification("cpus", ValueUtils.getValue(ResourceUtils.getUnreservedScalar("cpus", cpu)),
//           ROLE, PRINCIPAL),
//         new DefaultResourceSpecification("mem", ValueUtils.getValue(ResourceUtils.getUnreservedScalar("mem", mem)),
//           ROLE, PRINCIPAL),
//         new DefaultResourceSpecification("ports", ValueUtils.getValue(ResourceUtils.getUnreservedScalar("ports", 3)),
//           ROLE, PRINCIPAL))
//     }

//     private def getVolumes(disk: Double): Collection[VolumeSpecification] = {
//       val volumeSpecification = new DefaultVolumeSpecification(
//         disk, VolumeSpecification.Type.MOUNT, CONTAINER_PATH, ROLE, PRINCIPAL)
//       Arrays.asList(volumeSpecification)
//     }
//   }

//   class BrokerTaskTypeSpecification(
//     count: Int,
//     name: String,
//     command: Protos.CommandInfo,
//     resources: Collection[ResourceSpecification],
//     volumes: Collection[VolumeSpecification]) extends DefaultTaskTypeSpecification(count, name, command, resources, volumes) {

//     import BrokerTaskTypeSpecification._
//     override def getCommand(id: Int): Protos.CommandInfo = {
//       val cmdFmt = "export PATH=$(ls -d $MESOS_SANDBOX/jre*/bin):$PATH && " +
//       "$MESOS_SANDBOX/kafka_2.11-0.10.0.0/bin/kafka-server-start.sh " +
//       "$MESOS_SANDBOX/kafka_2.11-0.10.0.0/config/server.properties " +
//       "--override zookeeper.connect=master.mesos:2181/dcos-service-%s " +
//       "--override broker.id=%s " +
//       "--override log.dirs=$MESOS_SANDBOX/%s"
//       Protos.CommandInfo.newBuilder().
//         addUris(Protos.CommandInfo.URI.newBuilder().setValue(JAVA_URI)).
//         addUris(Protos.CommandInfo.URI.newBuilder().setValue(KAFKA_URI)).
//         setValue(cmdFmt format (SERVICE_NAME, id, CONTAINER_PATH)).
//         build()
//     }
//   }
// }

// object LosImplicits {
//   implicit class AsScalaOption[T](o: java.util.Optional[T]) {
//     def asScala: Option[T] =
//       if (o.isPresent()) Some(o.get) else None
//   }
// }

// class LeService( appConfiguration: AppConfiguration) extends Service {
//   val TWO_WEEK_SEC = 2 * 7 * 24 * 60 * 60
//   val logger = LoggerFactory.getLogger(getClass)

//   import LosImplicits._
//   def register(serviceSpecification: ServiceSpecification): Unit = {
//     val stateStore = new CuratorStateStore(serviceSpecification.getName, appConfiguration.zookeeper);

//     val defaultScheduler = new DefaultScheduler(serviceSpecification, appConfiguration.zookeeper)


//     def registerFramework(sched: Scheduler, frameworkInfo: Protos.FrameworkInfo , masterUri: String): Unit = {
//       logger.info("Registering framework: " + frameworkInfo);
//       val driver = new SchedulerDriverFactory().create(sched, frameworkInfo, masterUri);
//       driver.run()
//     }

//     val optionalFrameworkId = stateStore.fetchFrameworkId()
//     val frameworkInfo = {
//       val fwkInfoBuilder =
//         Protos.FrameworkInfo.newBuilder().
//           setName(serviceSpecification.getName()).
//           setFailoverTimeout(TWO_WEEK_SEC.toDouble).
//           setUser("root").
//           setRole(SchedulerUtils.nameToRole(serviceSpecification.getName())).
//           setPrincipal(SchedulerUtils.nameToPrincipal(serviceSpecification.getName())).
//           setCheckpoint(true)

//       optionalFrameworkId.asScala.foreach(fwkInfoBuilder.setId)

//       fwkInfoBuilder.build()
//     }

//     registerFramework(defaultScheduler, frameworkInfo, appConfiguration.master)
//   }
// }
