package org.vivint.ceph.model

import java.util.UUID

case class ServiceLocation(slaveId: String, hostname: String, ip: String, port: Int)

case class CephNode(
  id: UUID,
  cluster: String,
  role: String,
  paused: Boolean = false,
  slaveId: Option[String] = None, // This is set once the reservation is confirmed
  location: Option[ServiceLocation] = None) {
  def resourcesReserved = slaveId.nonEmpty
}
