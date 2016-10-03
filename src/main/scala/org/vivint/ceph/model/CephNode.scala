package org.vivint.ceph.model

import java.util.UUID

case class ServiceLocation(slaveId: String, hostname: String, ip: String, port: Int)

case class CephNode(
  id: UUID,
  cluster: String,
  role: String,
  paused: Boolean = false,
  reservationConfirmed: Boolean = false,
  slaveId: Option[String] = None,
  location: Option[ServiceLocation] = None) {
  if (reservationConfirmed)
    require(slaveId.nonEmpty)
  def resourcesReserved = slaveId.nonEmpty
}
