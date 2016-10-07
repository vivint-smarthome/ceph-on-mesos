package com.vivint.ceph
package model

object RunState extends lib.Enum {
  sealed trait EnumVal extends Value

  case object Paused extends EnumVal { val name = "paused" }
  case object Running extends EnumVal { val name = "running" }
  val values = Vector(Paused, Running)
}
