package cephui
package pages

import lib.Http
import japgolly.scalajs.react._
import japgolly.scalajs.react.vdom.prefix_<^._
import org.scalajs.dom
import elements._
import scalacss.Defaults._
import scalacss.ScalaCssReact._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Success,Failure}
object ConfigPage {

  object Style extends StyleSheet.Inline {
    import dsl._
    val content = style(textAlign.center,
      fontSize(30.px),
      minHeight(450.px),
      paddingTop(40.px))
  }

  case class State(config: Option[String], saving: Boolean = false)
  class Backend($: BackendScope[Unit, State]) {
    def start() = CallbackTo {
      Http.request[String]("GET", "/v1/config/deployment-config.conf").
        onComplete {
          case Success(cfg) => $.modState { ste => ste.copy(Some(cfg)) }.runNow()
          case Failure(ex) =>
            println(ex)
        }
    }

    def updateText(e: ReactEventI) = {
      val v = e.target.value
      $.modState { ste =>
        dom.console.log(e)
        ste.copy(config = Some(v))
      }
    }


    def saveConfig() =
      $.modState { ste =>
        ste.config match {
          case Some(cfg) =>
            Http.request[Unit]("PUT", "/v1/config/deployment-config.conf",
              headers = Map("Content-Type" -> "application/text"),
              data = cfg).
              onComplete { _ =>
                dom.window.alert("success")
                $.modState(_.copy(saving = false)).runNow()
              }
            ste.copy(saving = true)
          case None =>
            ste
        }
      }.runNow()

    def render(s: State) =
      <.div(
        s.config.map { cfg =>
          Grid()(
            Row()(
              <.textarea(
                ^.className := "col-xs-12",
                ^.rows := 25,
                ^.defaultValue := cfg,
                ^.disabled := s.saving,
                ^.onChange ==> updateText)),
            Row()(
              Col(
                xs = 4)(
                Button(
                  bsStyle = "success",
                  disabled = s.saving,
                  onClick = { () => saveConfig() })("Save Changes"))
            )
          )
        }
      )
  }

val component = ReactComponentB[Unit]("ConfigPage").
  initialState(State(None)).
  renderBackend[Backend].
  componentDidMount(_.backend.start()).
  build


  def apply() = component()
}
