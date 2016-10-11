package com.vivint.ceph
import org.apache.mesos.Protos._
import scala.collection.immutable.NumericRange
import scala.concurrent.duration._
import scala.collection.JavaConversions._
import scala.collection.breakOut

object ProtoHelpers {
  def newFilters(refuseDuration: Option[FiniteDuration]): Filters = {
    val b = Filters.newBuilder
    refuseDuration.foreach { d =>
      b.setRefuseSeconds(d.toMillis / 1000.0)
    }
    b.build
  }

  def newDisk(source: Option[Resource.DiskInfo.Source]): Resource.DiskInfo = {
    val b = Resource.DiskInfo.newBuilder
    source.foreach(b.setSource)
    b.build
  }

  sealed trait OfferOperationMagnet {
    def apply(b: Offer.Operation.Builder): Offer.Operation.Builder
  }
  case class UnreserveOperationMagnet(operation: Offer.Operation.Unreserve) extends OfferOperationMagnet {
    def apply(b: Offer.Operation.Builder): Offer.Operation.Builder = {
      b.
        setType(Offer.Operation.Type.UNRESERVE).
        setUnreserve(operation)
    }
  }
  case class DestroyOperationMagnet(destroy: Offer.Operation.Destroy) extends OfferOperationMagnet {
    def apply(b: Offer.Operation.Builder): Offer.Operation.Builder = {
      b.
        setType(Offer.Operation.Type.DESTROY).
        setDestroy(destroy)
    }
  }
  case class CreateOperationMagnet(create: Offer.Operation.Create) extends OfferOperationMagnet {
    def apply(b: Offer.Operation.Builder): Offer.Operation.Builder = {
      b.
        setType(Offer.Operation.Type.CREATE).
        setCreate(create)
    }
  }
  case class ReserveOperationMagnet(reserve: Offer.Operation.Reserve) extends OfferOperationMagnet {
    def apply(b: Offer.Operation.Builder): Offer.Operation.Builder = {
      b.
        setType(Offer.Operation.Type.RESERVE).
        setReserve(reserve)
    }
  }
  case class LaunchOperationMagnet(launch: Offer.Operation.Launch) extends OfferOperationMagnet {
    def apply(b: Offer.Operation.Builder): Offer.Operation.Builder = {
      b.
        setType(Offer.Operation.Type.LAUNCH).
        setLaunch(launch)
    }
  }
  object OfferOperationMagnet {
    import scala.language.implicitConversions
    implicit def fromUnreserve(r: Offer.Operation.Unreserve): OfferOperationMagnet = UnreserveOperationMagnet(r)
    implicit def fromDestroy(r: Offer.Operation.Destroy): OfferOperationMagnet = DestroyOperationMagnet(r)
    implicit def fromCreate(r: Offer.Operation.Create): OfferOperationMagnet = CreateOperationMagnet(r)
    implicit def fromReserve(r: Offer.Operation.Reserve): OfferOperationMagnet = ReserveOperationMagnet(r)
    implicit def fromLaunch(r: Offer.Operation.Launch): OfferOperationMagnet = LaunchOperationMagnet(r)
  }

  def newOfferOperation(operation: OfferOperationMagnet) = {
    val b = Offer.Operation.newBuilder
    operation(b)
    b.build
  }

  def newUnreserveOperation(resources: Iterable[Resource]): Offer.Operation.Unreserve = {
    val b = Offer.Operation.Unreserve.newBuilder
    resources.foreach(b.addResources)
    b.build
  }

  def newDestroyOperation(resources: Iterable[Resource]): Offer.Operation.Destroy = {
    val b = Offer.Operation.Destroy.newBuilder
    resources.foreach(b.addVolumes)
    b.build
  }

  def newLaunchOperation(taskInfos: Iterable[TaskInfo]): Offer.Operation.Launch = {
    val b = Offer.Operation.Launch.newBuilder
    taskInfos.foreach(b.addTaskInfos)
    b.build
  }

  def newLabels(kvs: (String, String)*): Labels = {
    val b = Labels.newBuilder

    kvs.foreach { case (key, value) =>
      b.addLabels(newLabel(key,value))
    }
    b.build
  }

  def newLabel(key: String, value: String): Label = {
    Label.newBuilder.setKey(key).setValue(value).build
  }

  def newTaskId(taskId: String): TaskID = {
    TaskID.newBuilder.
      setValue(taskId).
      build
  }

  def newVolume(containerPath: String, hostPath: String, mode: Volume.Mode = Volume.Mode.RW) = {
    Volume.newBuilder.
      setContainerPath(containerPath).
      setHostPath(hostPath).
      setMode(mode).
      build
  }

  def newVariable(name: String, value: String) =
    Environment.Variable.newBuilder.
      setName(name).
      setValue(value).
      build

  def newEnvironment(vars: (String, String)*) = {
    val b = Environment.newBuilder
    vars.foreach { case (name, value) => b.addVariables(newVariable(name, value)) }
    b.build
  }


  def newRanges(ranges: Iterable[NumericRange.Inclusive[Long]]): Value.Ranges = {
    val b = Value.Ranges.newBuilder
    ranges.foreach { r =>
      b.addRange(
        Value.Range.newBuilder.
          setBegin(r.min).
          setEnd(r.max))
    }
    b.build
  }

  def newScalar(amount: Double): Value.Scalar = {
    Value.Scalar.newBuilder.setValue(amount).build
  }

  def newOfferId(id: String): OfferID = {
    OfferID.newBuilder.setValue(id).build
  }

  def newFrameworkId(id: String): FrameworkID = {
    FrameworkID.newBuilder.setValue(id).build
  }

  def newSlaveId(id: String): SlaveID = {
    SlaveID.newBuilder.setValue(id).build
  }


  def newRangesResource(name: String, ranges: Iterable[NumericRange.Inclusive[Long]], role: String): Resource =
    Resource.newBuilder.
      setName(name).
      setRole(role).
      setType(Value.Type.RANGES).
      setRanges(newRanges(ranges)).
      build


  def newScalarResource(name: String, amount: Double, role: String = "*",
    reservation: Option[Resource.ReservationInfo] = None, disk: Option[Resource.DiskInfo] = None): Resource = {
    val b = Resource.newBuilder.
      setName(name).
      setScalar(newScalar(amount)).
      setType(Value.Type.SCALAR).
      setRole(role)
    reservation.foreach(b.setReservation)
    disk.foreach(b.setDisk)
    b.build
  }

  def newTaskStatus(taskId: String, slaveId: String, state: TaskState = TaskState.TASK_LOST): TaskStatus =
    TaskStatus.newBuilder.
      setTaskId(TaskID.newBuilder.setValue(taskId)).
      setSlaveId(SlaveID.newBuilder.setValue(slaveId)).
      setState(state).
      build

  implicit class RichOffer(offer: Offer) {
    def resources =
      offer.getResourcesList.toList

    def withResources(resources: Iterable[Resource]): Offer = {
      val b = offer.toBuilder.clearResources()
      resources.foreach(b.addResources)
      b.build
    }

    def slaveId: Option[String] =
      if (offer.hasSlaveId)
        Some(offer.getSlaveId.getValue)
      else
        None

    def hostname: Option[String] =
      if (offer.hasHostname)
        Some(offer.getHostname)
      else
        None
  }

  implicit class RichReservationInfo(reservation: Resource.ReservationInfo) {
    def labels =
      if (reservation.hasLabels) Some(reservation.getLabels) else None
  }

  implicit class RichEnvironment(env: Environment) {
    def get(key: String): Option[String] =
      env.getVariablesList.iterator.collectFirst {
        case variable if variable.getName == key => variable.getValue
      }

    def toMap: Map[String, String] = {
      env.getVariablesList.map { l =>
        l.getName -> l.getValue
      }(breakOut)
    }
  }

  implicit class RichResource(resource: Resource) {

    def reservation =
      if (resource.hasReservation) Some(resource.getReservation) else None

    def diskSourceOption: Option[Resource.DiskInfo.Source] =
      if (resource.hasDisk && resource.getDisk.hasSource)
        Some(resource.getDisk.getSource)
      else
        None
    def ranges: List[NumericRange.Inclusive[Long]] =
      if (resource.hasRanges) {
        resource.getRanges.getRangeList.toList.map { r =>
          NumericRange.inclusive(r.getBegin, r.getEnd, 1)
        }
      } else
        Nil
  }

  implicit class RichLabels(labels: Labels) {
    def get(key: String): Option[String] =
      labels.getLabelsList.iterator.collectFirst {
        case label if label.getKey == key => label.getValue
      }

    def toMap: Map[String, String] = {
      labels.getLabelsList.map { l =>
        l.getKey -> l.getValue
      }(breakOut)
    }
  }

  implicit class RichLaunch(launch: Offer.Operation.Launch) {
    def tasks: List[TaskInfo] = {
      launch.getTaskInfosList.toList
    }
  }

}
