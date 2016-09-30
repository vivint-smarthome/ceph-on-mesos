package org.vivint.ceph.model
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

  implicit val ServiceLocationFormat = Json.format[ServiceLocation]
  implicit val MonTaskFormat = Json.format[CephNode]
  implicit val ClusterSecretsFormat = Json.format[ClusterSecrets]
}
