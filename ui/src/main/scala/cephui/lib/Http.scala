package cephui.lib

import scala.concurrent.{Future,Promise}
import org.scalajs.dom
import scala.util.Try
import scala.scalajs.js

object Http {
  sealed class HttpFailure(msg: String) extends Exception(msg)
  case class RequestFailure(req: dom.XMLHttpRequest) extends HttpFailure(
    s"response ${req.status} received; ${req.responseText}")
  case class MarshallingFailure(str: String) extends HttpFailure(str)

  trait Unmarshaller[T] extends (dom.XMLHttpRequest => T)
  object Unmarshaller {
    implicit def stringUnmarshaller = new Unmarshaller[String] {
      def apply(e: dom.XMLHttpRequest) = e.responseText
    }
    implicit def unitUnmarshaller = new Unmarshaller[Unit] {
      def apply(e: dom.XMLHttpRequest) = ()
    }
  }

  def request[T](method: String, uri: String, headers: Map[String, String] = Map.empty, data: js.Any = js.undefined )(
    implicit um: Unmarshaller[T]): Future[T] = {
    val p = Promise[T]
    val xhr = new dom.XMLHttpRequest()
    xhr.open(method, uri)
    headers.foreach { case (h, v) =>
      xhr.setRequestHeader(h, v)
    }

    xhr.onload = { (e: dom.Event) =>
      if ((200 until 300) contains xhr.status) {
        p.complete(Try(um.apply(xhr)))
      } else {
        p.failure(RequestFailure(xhr))
      }
    }
    xhr.send(data)
    p.future
  }
}
