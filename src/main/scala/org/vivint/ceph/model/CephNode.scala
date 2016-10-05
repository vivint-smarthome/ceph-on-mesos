package org.vivint.ceph
package model

import java.util.UUID

object RunState extends lib.Enum {
  sealed trait EnumVal extends Value

  case object Paused extends EnumVal { val name = "paused" }
  case object Running extends EnumVal { val name = "running" }
}

object NodeRole extends lib.Enum {
  sealed trait EnumVal extends Value

  case object Monitor extends EnumVal { val name = "mon" }
}



case class ServiceLocation(slaveId: String, hostname: String, ip: String, port: Int)

case class CephNode(
  id: UUID,
  cluster: String,
  role: NodeRole.EnumVal,
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
