package org.vivint.ceph.model
import play.api.libs.json._

object PlayJsonFormats{
  implicit val ServiceLocationFormat = Json.format[ServiceLocation]
  implicit val MonTaskFormat = Json.format[CephNode]
}
