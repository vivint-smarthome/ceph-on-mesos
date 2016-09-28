package org.vivint.ceph
import scala.concurrent.duration._

object ProtoHelpers {
  import org.apache.mesos.Protos._
  def filters(refuseDuration: Option[FiniteDuration]): Filters = {
    val b = Filters.newBuilder
    refuseDuration.foreach { d =>
      b.setRefuseSeconds(d.toMillis / 1000.0)
    }
    b.build
  }

}
