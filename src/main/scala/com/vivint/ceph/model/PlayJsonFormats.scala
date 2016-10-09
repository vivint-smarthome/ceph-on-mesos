package com.vivint.ceph
package model
import akka.util.ByteString
import java.util.Base64
import play.api.libs.json._

object PlayJsonFormats{
  implicit val ByteStringFormat = new Format[ByteString] {
    val base64Decoder = Base64.getDecoder
    val base64Encoder = Base64.getEncoder

    def reads(js: JsValue): JsResult[ByteString] =
      js.validate[String].map { str =>
        ByteString(base64Decoder.decode(str))
      }

    def writes(byteString: ByteString): JsValue =
      JsString(
        base64Encoder.encodeToString(byteString.toArray))
  }

  def enumFormat[T <: lib.Enum](e: T): Format[T#EnumVal] = new Format[T#EnumVal] {
    def reads(js: JsValue): JsResult[T#EnumVal] = js match {
      case JsString(str) =>
        e.values.find(_.name == str) match {
          case Some(enumVal) => JsSuccess(enumVal)
          case None =>
            JsError(s"invalid value for ${e}: '${str}'. Valid values are ${e.values.map { v => "'" + v.name + "'" }.mkString(",")}")
        }
      case other =>
        JsError(s"string expected, got ${other}")
    }

    def writes(enumVal: T#EnumVal): JsValue =
      JsString(enumVal.name)
  }

  implicit val RunStateFormat = enumFormat(RunState)
  implicit val TaskRoleFormat = enumFormat(TaskRole)
  implicit val ServiceLocationFormat = Json.format[ServiceLocation]
  implicit val MonTaskFormat = Json.format[PersistentState]
  implicit val ClusterSecretsFormat = Json.format[ClusterSecrets]

  implicit val TaskWriter = Writes[Task] { task =>
    Json.toJson(task.pState).as[JsObject] ++
    Json.obj(
      "version" -> task.version,
      "persistentVersion" -> task.persistentVersion,
      "behavior" -> task.behavior.name,
      "wantingNewOffer" -> task.wantingNewOffer,
      "taskStatus" -> task.taskStatus.map(_.state.name)
    )

  }

}
