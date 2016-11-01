package com.vivint.ceph.api.model

import play.api.libs.json._
object ApiPlayJsonFormats {
  implicit val ErrorResponseFormat = Json.format[ErrorResponse]
}
