package mesosphere.marathon.core.launcher.impl

import org.apache.mesos.{ Protos => MesosProtos }

object LabelsSerializer {
  def toMesos(labels: Map[String, String]): Iterable[MesosProtos.Label] = {
    for {
      (key, value) <- labels
    } yield MesosProtos.Label.newBuilder.setKey(key).setValue(value).build
  }

  def toMesosLabelsBuilder(labels: Map[String, String]): MesosProtos.Labels.Builder = {
    val builder = MesosProtos.Labels.newBuilder
    toMesos(labels).foreach(builder.addLabels)
    builder
  }
}

/**
  * Encapsulates information about a reserved resource and its (probably empty) list of reservation labels.
  */
case class ReservationLabels(labels: Map[String, String]) {
  lazy val mesosLabels: MesosProtos.Labels = {
    LabelsSerializer.toMesosLabelsBuilder(labels).build
  }

  def get(key: String): Option[String] = labels.get(key)

  override def toString: String = labels.map { case (k, v) => s"$k: $v" }.mkString(", ")
}

object ReservationLabels {
  def withoutLabels: ReservationLabels = new ReservationLabels(Map.empty)

  def apply(resource: MesosProtos.Resource): ReservationLabels = {
    if (resource.hasReservation && resource.getReservation.hasLabels)
      ReservationLabels(resource.getReservation.getLabels)
    else
      ReservationLabels.withoutLabels
  }
  def apply(labels: MesosProtos.Labels): ReservationLabels = {
    import scala.collection.JavaConverters._
    ReservationLabels(labels.getLabelsList.asScala.iterator.map(l => l.getKey -> l.getValue).toMap)
  }
}
