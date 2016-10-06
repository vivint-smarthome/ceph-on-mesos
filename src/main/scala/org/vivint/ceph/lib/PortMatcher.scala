package org.vivint.ceph.lib

import mesosphere.mesos.matcher.PortsMatchResult
import mesosphere.mesos.matcher.PortsMatchResult.PortWithRole
import mesosphere.mesos.matcher.ResourceMatcher.ResourceSelector
import mesosphere.mesos.matcher.{ MatchResult, ResourceMatcher }
import mesosphere.mesos.protos.Resource
import org.apache.mesos.Protos
import scala.annotation.tailrec
import scala.collection.JavaConversions._
import org.vivint.ceph.ProtoHelpers

class SinglePortMatcher(resourceSelector: ResourceSelector) extends ResourceMatcher {
  import ProtoHelpers._
  val resourceName = Resource.PORTS
  def apply(offerId: String, resources: Iterable[Protos.Resource]): Iterable[MatchResult] = {
    (for {
      resource <- resources.filter(resourceSelector(_)).headOption
      range <- resource.ranges.headOption
    } yield {
      val port = range.min

      // ReservationInfo
      List(
        PortsMatchResult.apply(
          true,
          List(Some(PortWithRole("main", port.toInt, resource.reservation))),
          List(
            resource.toBuilder().
              setRanges(newRanges(List(port to port)))
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

class ContiguousPortMatcher(ports: Int, resourceSelector: ResourceSelector) extends ResourceMatcher {
  import ProtoHelpers._
  val resourceName = Resource.PORTS
  def apply(offerId: String, resources: Iterable[Protos.Resource]): Iterable[MatchResult] =
    doMatch(resources.toList)

  @tailrec private def doMatch(resources: List[Protos.Resource]): Iterable[MatchResult] = resources match {
    case Nil =>
      List(
        PortsMatchResult(
          false,
          Nil,
          Nil))
    case resource :: rest =>
      if (!resourceSelector(resource))
        doMatch(rest)
      else
        resource.ranges.find(_.length >= ports) match {
          case Some(range) =>
            val reserveRange = range.min to (range.min + ports - 1)
            // ReservationInfo
            List(
              PortsMatchResult.apply(
                true,
                reserveRange.map { n => Some(PortWithRole("main", n.toInt, resource.reservation)) },
                List(
                  resource.toBuilder.
                    setRanges(
                      newRanges(List(reserveRange))).
                    build)))
          case None =>
            doMatch(rest)
        }
  }
}
