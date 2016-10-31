package com.vivint.ceph

package object lib {

  import java.time.ZonedDateTime

  implicit val zonedDateTimeOrdering = new Ordering[ZonedDateTime] {
    def compare(a: ZonedDateTime, b: ZonedDateTime): Int =
      a.compareTo(b)
  }
}
