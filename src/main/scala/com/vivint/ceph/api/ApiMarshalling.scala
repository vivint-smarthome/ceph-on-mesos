package com.vivint.ceph.api

import akka.http.scaladsl.marshalling.{Marshaller, ToEntityMarshaller}
import akka.http.scaladsl.model.ParsingException
import akka.http.scaladsl.unmarshalling.{FromEntityUnmarshaller, Unmarshaller}
import akka.http.scaladsl.model.MediaTypes
import com.vivint.ceph.model.{ Job, PlayJsonFormats, RunState, ReservationReleaseDetails }
import model.ErrorResponse
import play.api.libs.json._

object ApiMarshalling {
  def fromJsonResponse[T](implicit reader: Reads[T]): FromEntityUnmarshaller[T] =
    Unmarshaller.stringUnmarshaller.
      forContentTypes(MediaTypes.`application/json`).
      map { str => Json.parse(str).as[T] }

  def toJsonResponse[T](implicit writer: Writes[T]): ToEntityMarshaller[T] =
    Marshaller.stringMarshaller(MediaTypes.`application/json`).
      compose { data: T =>
        Json.stringify(Json.toJson(data)) }

  import PlayJsonFormats._
  import model.ApiPlayJsonFormats._
  implicit val jobsWriter = toJsonResponse[Iterable[Job]]
  implicit val reservationReleaseWriter = toJsonResponse[Iterable[ReservationReleaseDetails]]
  implicit val errorWriter = toJsonResponse[ErrorResponse]

  def uuidFromString(str: String) =
    try {
      java.util.UUID.fromString(str)
    } catch {
      case ex: IllegalArgumentException =>
        throw ParsingException(s"Couldn't parse UUID: ${ex.getMessage}")
    }

  def runStateFromString(str: String) =
    RunState.values.find(_.name == str).getOrElse {
      throw ParsingException(s"invalid runState '${str}', expected one of '${RunState.values.mkString(", ")}'")
    }


  implicit val runStateTextReader: Unmarshaller[String, RunState.EnumVal] =
    Unmarshaller.strict[String, RunState.EnumVal](runStateFromString)
}

