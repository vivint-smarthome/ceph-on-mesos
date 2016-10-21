package cephui.models

import json._

object JsFormats {
  implicit val locationAccessor = ObjectAccessor.create[Location]
  implicit val jobAccessor = ObjectAccessor.create[Job]
}
