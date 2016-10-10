package com.vivint.ceph
package model

object TaskRole extends lib.Enum {
  sealed trait EnumVal extends Value

  case object Monitor extends EnumVal { val name = "mon" }
  case object OSD extends EnumVal { val name = "osd" }
  case object RGW extends EnumVal { val name = "rgw" }
  val values = Vector(Monitor, OSD, RGW)
}
