package cephui.models

case class Location(
  hostname: Option[String],
  ip: Option[String],
  port: Option[Int])

case class Job(
  id: String,
  cluster: String,
  role: String,
  goal: Option[String],
  lastLaunched: Option[String],
  reservationConfirmed: Boolean,
  reservationId: Option[String],
  slaveId: Option[String],
  taskId: Option[String],
  location: Location,
  version: Int,
  persistentVersion: Int,
  behavior: String,
  wantingNewOffer: Boolean,
  taskStatus: Option[String])
