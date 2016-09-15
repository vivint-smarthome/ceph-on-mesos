package org.vivint.ceph

import com.google.protobuf.ByteString
import org.rogach.scallop._
import org.apache.mesos._
import org.apache.mesos.Protos._
import scaldi.Injector
import scaldi.Injectable._
//remove if not needed
import scala.collection.JavaConversions._

class TestScheduler(private val implicitAcknowledgements: Boolean, private val executor: ExecutorInfo, private val totalTasks: Int)(implicit val injector: Injector)
     extends Scheduler {

  private val frameworkIdSetter = inject[FrameworkId]
   override def registered(driver: SchedulerDriver, frameworkId: FrameworkID, masterInfo: MasterInfo): Unit = {
     frameworkIdSetter.set(frameworkId)
     println("Registered! ID = " + frameworkId.getValue)
   }

   override def reregistered(driver: SchedulerDriver, masterInfo: MasterInfo): Unit = {
   }

   override def disconnected(driver: SchedulerDriver): Unit = {
   }

   override def resourceOffers(driver: SchedulerDriver, offers: java.util.List[Offer]): Unit = {
     val CPUS_PER_TASK = 1.0
     val MEM_PER_TASK = 128.0
     for (offer <- offers) {
       val launch = Offer.Operation.Launch.newBuilder()
       var offerCpus = 0.0
       var offerMem = 0.0
       for (resource <- offer.getResourcesList) {
         if (resource.getName == "cpus") {
           offerCpus += resource.getScalar.getValue
         } else if (resource.getName == "mem") {
           offerMem += resource.getScalar.getValue
         }
       }
       println("Received offer " + offer.getId.getValue + " with cpus: " +
         offerCpus +
         " and mem: " +
         offerMem)
       var remainingCpus = offerCpus
       var remainingMem = offerMem
       while (launchedTasks < totalTasks && remainingCpus >= CPUS_PER_TASK &&
         remainingMem >= MEM_PER_TASK) {
         launchedTasks += 1
         val taskId = TaskID.newBuilder().setValue(java.lang.Integer.toString(launchedTasks))
           .build()
         println("Launching task " + taskId.getValue + " using offer " +
           offer.getId.getValue)
         val task = TaskInfo.newBuilder().setName("task " + taskId.getValue)
           .setTaskId(taskId)
           .setSlaveId(offer.getSlaveId)
           .addResources(Resource.newBuilder().setName("cpus").setType(Value.Type.SCALAR)
             .setScalar(Value.Scalar.newBuilder().setValue(CPUS_PER_TASK)))
           .addResources(Resource.newBuilder().setName("mem").setType(Value.Type.SCALAR)
             .setScalar(Value.Scalar.newBuilder().setValue(MEM_PER_TASK)))
           .setExecutor(ExecutorInfo.newBuilder(executor))
           .build()
         launch.addTaskInfos(TaskInfo.newBuilder(task))
         remainingCpus -= CPUS_PER_TASK
         remainingMem -= MEM_PER_TASK
       }
       val offerIds = new java.util.ArrayList[OfferID]()
       offerIds.add(offer.getId)
       val operations = new java.util.ArrayList[Offer.Operation]()
       val operation = Offer.Operation.newBuilder().setType(Offer.Operation.Type.LAUNCH)
         .setLaunch(launch)
         .build()
       operations.add(operation)
       val filters = Filters.newBuilder().setRefuseSeconds(1).build()
       driver.acceptOffers(offerIds, operations, filters)
     }
   }

   override def offerRescinded(driver: SchedulerDriver, offerId: OfferID): Unit = {
   }

   override def statusUpdate(driver: SchedulerDriver, status: TaskStatus): Unit = {
     println("Status update: task " + status.getTaskId.getValue + " is in state " +
       status.getState.getValueDescriptor.getName)
     if (status.getState == TaskState.TASK_FINISHED) {
       finishedTasks += 1
       println("Finished tasks: " + finishedTasks)
       if (finishedTasks == totalTasks) {
         driver.stop()
       }
     }
     if (status.getState == TaskState.TASK_LOST || status.getState == TaskState.TASK_KILLED ||
       status.getState == TaskState.TASK_FAILED) {
       System.err.println("Aborting because task " + status.getTaskId.getValue +
         " is in unexpected state " +
         status.getState.getValueDescriptor.getName +
         " with reason '" +
         status.getReason.getValueDescriptor.getName +
         "'" +
         " from source '" +
         status.getSource.getValueDescriptor.getName +
         "'" +
         " with message '" +
         status.getMessage +
         "'")
       driver.abort()
     }
     if (!implicitAcknowledgements) {
       driver.acknowledgeStatusUpdate(status)
     }
   }

   override def frameworkMessage(driver: SchedulerDriver,
     executorId: ExecutorID,
     slaveId: SlaveID,
     data: Array[Byte]): Unit = {
   }

   override def slaveLost(driver: SchedulerDriver, slaveId: SlaveID): Unit = {
   }

   override def executorLost(driver: SchedulerDriver,
     executorId: ExecutorID,
     slaveId: SlaveID,
     status: Int): Unit = {
   }

   def error(driver: SchedulerDriver, message: String): Unit = {
     println("Error: " + message)
   }

   private var launchedTasks: Int = 0

   private var finishedTasks: Int = 0
}
