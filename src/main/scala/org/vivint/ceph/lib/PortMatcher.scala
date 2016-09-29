package org.vivint.ceph.lib

import mesosphere.mesos.matcher.PortsMatchResult
import mesosphere.mesos.matcher.PortsMatchResult.PortWithRole
import mesosphere.mesos.matcher.ResourceMatcher.ResourceSelector
import mesosphere.mesos.matcher.{ MatchResult, ResourceMatcher }
import mesosphere.mesos.protos.Resource
import org.apache.mesos.Protos
import scala.collection.JavaConversions._
import org.vivint.ceph.ProtoHelpers

class SinglePortMatcher(resourceSelector: ResourceSelector) extends ResourceMatcher {
  import ProtoHelpers._
  val resourceName = Resource.PORTS
  def apply(offerId: String, resources: Iterable[Protos.Resource]): Iterable[MatchResult] = {
    (for {
      resource <- resources.filter(resourceSelector(_)).headOption
      range <- resource.getRanges.getRangeList.headOption
    } yield {
      val port = range.getBegin

      // ReservationInfo
      List(
        PortsMatchResult.apply(
          true,
          List(Some(PortWithRole("main", port.toInt, resource.reservation))),
          List(
            resource.toBuilder().
              setRanges(newRanges(List(port -> port)))
              .build)))
    }) getOrElse {
      List(
        PortsMatchResult(
          false,
          Nil,
          Nil))
    }
  }
}
