package com.vivint.ceph.api

import akka.http.scaladsl.marshalling.{Marshaller, ToEntityMarshaller}
import akka.http.scaladsl.model.MediaTypes
import play.api.libs.json._

object ApiMarshalling {

  implicit def toJsonResponse[T](implicit writer: Writes[T]): ToEntityMarshaller[T] =
    Marshaller.stringMarshaller(MediaTypes.`application/json`).
      compose { data: T =>
        Json.stringify(Json.toJson(data)) }

}

