package org.vivint.ceph.model

import java.util.UUID

sealed trait CephNode
case class ServiceLocation(slaveId: String, address: String, port: String)

case class MonNode(id: UUID, reservationId: Option[UUID], location: ServiceLocation) extends CephNode

object PlayJsonFormats{
  import play.api.libs.json._
  implicit val ServiceLocationFormat = Json.format[ServiceLocation]
  implicit val MonTaskFormat = Json.format[MonNode]
}
