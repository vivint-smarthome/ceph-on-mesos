package com.vivint.ceph
package model

import java.util.UUID

case class ServiceLocation(hostname: String, ip: String, port: Int)

case class PersistentState(
  id: UUID,
  cluster: String,
  role: TaskRole.EnumVal,
  goal: Option[RunState.EnumVal] = None,
  lastLaunched: Option[RunState.EnumVal] = None,
  reservationConfirmed: Boolean = false,
  slaveId: Option[String] = None,
  location: Option[ServiceLocation] = None) {
  if (reservationConfirmed)
    require(slaveId.nonEmpty)

  def resourcesReserved =
    slaveId.nonEmpty
}
