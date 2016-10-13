package com.vivint.ceph
package model

import java.util.UUID

case class PersistentState(
  id: UUID,
  cluster: String,
  role: JobRole.EnumVal,
  goal: Option[RunState.EnumVal] = None,
  lastLaunched: Option[RunState.EnumVal] = None,
  reservationConfirmed: Boolean = false,
  reservationId: Option[UUID] = None,
  slaveId: Option[String] = None,
  taskId: Option[String] = None,
  location: Location = Location.empty) {

  def ipLocation: Option[IPLocationLike] = location match {
    case i: IPLocationLike => Some(i)
    case _ => None
  }

  def serviceLocation: Option[ServiceLocation] = location match {
    case s: ServiceLocation => Some(s)
    case _ => None
  }

  if (reservationConfirmed) {
    require(slaveId.nonEmpty, "slaveId must be set if reservationConfirmed is set")
    require(reservationId.nonEmpty, "reservationId must be set if reservationConfirmed is set")
  }

  def resourcesReserved =
    slaveId.nonEmpty
}
