package cephui

import japgolly.scalajs.react._
import org.scalajs.dom

import scala.scalajs.js.JSApp
import scala.scalajs.js.annotation.JSExport
import cephui.css.AppCSS
import cephui.routes.AppRouter

@JSExport
object ReactApp extends JSApp {

  @JSExport
  override def main(): Unit = {
    AppCSS.load
    AppRouter.router().render(dom.document.getElementById("ceph-ui"))
  }
}
