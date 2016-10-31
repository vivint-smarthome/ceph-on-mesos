package cephui.models

case class DanglingReservation(
  id: String,
  lastSeen: String,
  unreserve: Boolean = false,
  details: Option[String]    = None)
