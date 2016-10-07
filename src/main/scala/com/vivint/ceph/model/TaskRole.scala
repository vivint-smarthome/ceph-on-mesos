package com.vivint.ceph
package model

object TaskRole extends lib.Enum {
  sealed trait EnumVal extends Value

  case object Monitor extends EnumVal { val name = "mon" }
  case object OSD extends EnumVal { val name = "osd" }
  val values = Vector(Monitor, OSD)
}
