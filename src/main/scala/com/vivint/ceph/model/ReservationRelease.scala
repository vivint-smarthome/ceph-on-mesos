package com.vivint.ceph.model

import java.util.UUID
import java.time.ZonedDateTime

case class ReservationRelease(
  id: UUID,
  lastSeen: ZonedDateTime,
  unreserve: Boolean) {
  def toDetailed(details: Option[String] = None) =
    ReservationReleaseDetails(
      id = id,
      unreserve = unreserve,
      lastSeen = lastSeen,
      details = details)
}

case class ReservationReleaseDetails(
  id: UUID,
  lastSeen: ZonedDateTime,
  unreserve: Boolean = false,
  details: Option[String]    = None) {

  def withoutDetails =
    ReservationRelease(
      id = id,
      unreserve = unreserve,
      lastSeen = lastSeen)
}
