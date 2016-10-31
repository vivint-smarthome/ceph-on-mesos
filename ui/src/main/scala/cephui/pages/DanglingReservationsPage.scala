package cephui
package pages

import cephui.css.AppCSS
import japgolly.scalajs.react._
import japgolly.scalajs.react.extra.Reusability
import japgolly.scalajs.react.vdom.prefix_<^._
import scalacss.Defaults._
import scala.scalajs.js
import scalacss.ScalaCssReact._
import org.scalajs.dom
import models.DanglingReservation
import json._
import scala.concurrent.duration._
import scala.concurrent.{Future,Promise}
import scala.concurrent.ExecutionContext.Implicits.global
import models.JsFormats._
import lib.Http
import scala.util.{Success, Failure}

object DanglingReservationsPage {

  object Style extends StyleSheet.Inline {
    import dsl._
    val content = style(textAlign.center,
      fontSize(30.px),
      minHeight(450.px),
      paddingTop(40.px))
  }


  case class State(dangling: Seq[DanglingReservation], expanded: Set[String] = Set.empty)

  import elements.{Table, Grid, Row, Col, Button, Alert, Glyphicon}

  implicit val danglingReservationsDeserializer = new Http.Unmarshaller[Seq[DanglingReservation]] {
    def apply(xhr: dom.XMLHttpRequest): Seq[DanglingReservation] =
      JValue.fromString(xhr.responseText).toObject[Seq[DanglingReservation]]
  }

  def unreserve(reservation: DanglingReservation): Unit =
    Http.request[Unit]("PUT", s"/v1/reservation-reaper/${reservation.id}/unreserve").
      onComplete {
        case Success(_) =>
          dom.console.log(s"reservation ${reservation.id} set to be unreserved")
        case Failure(ex) =>
          dom.console.log(ex.getMessage)
      }

  val danglingReservationDetails = ReactComponentB[DanglingReservation]("DanglingReservationDetails").
    render_P { danglingReservation =>
      Row()(
        Col(xs = 1)(),
        Col(xs = 6)(
          Table()(
            <.thead(
              <.tr(
                <.th("Field"),
                <.th("Value"))),
            <.tbody(
              <.tr(<.td("ID"), <.td(danglingReservation.id)),
              <.tr(<.td("details"), <.td(
                <.pre(danglingReservation.details.getOrElse[String](""))))))),
        Col(
          xs = 4)(
          Button(bsStyle = "warning",
            onClick = { () => unreserve(danglingReservation) })("Destroy")))
    }.
    build

  class Backend($: BackendScope[Unit, State]) {
    private var running = true

    def poll(): Unit =
      Http.request[Seq[DanglingReservation]]("GET", "/v1/reservation-reaper/dangling").
        onComplete {
          case Success(danglingReservations) =>
            js.timers.setTimeout(3.seconds) {
              if (running) poll()
            }
            $.modState { ste => State(danglingReservations, ste.expanded)}.runNow()
          case Failure(ex) =>
            println(ex.getMessage)
            ex.printStackTrace(System.out)
        }

    def start() = CallbackTo {
      dom.console.log("le start")
      running = true
      poll()
    }

    def clear() = CallbackTo {
      dom.console.log("le end")
      running = false
    }

    def render(s: State) =
      <.div(
        Row()(
          Col(xs = 1)(),
          Col(xs = 10)(
            <.p("""
              |Dangling Reservations are resource reservations that are associated with a job that the framework doesn't
              |recognize. They can occur in the following scenarios:
              |""".stripMargin),
            <.ul(
              <.li("A job is deleted manually from the framework's persistent store, and the framework is restarted."),
              <.li("An issue with the persistence layer or the framework itself.")),
            <.p("""
              |In order to prevent ceph-on-mesos from deleting important persistent data in these cases, reservations
              |are listed here so they can be marked to be unreserved manually once the framework operator has confirmed
              |that the resources are in fact okay to release. Restoring jobs with these resource reservations is a
              |manual effort.
              |""".stripMargin),
            <.p("""
              |Resources are not unreserved immediately; the framework must wait until the reservation is offered again,
              |which could take several minutes. To accelerate this process you can try restarting the framework.
              |""".stripMargin))),
        Row()(
          Col(xs = 1)(),
          Col(xs = 10)(
            if(s.dangling.isEmpty) {
              Alert(bsStyle = "success")("There are no dangling reservations")
            } else {
              Table()(
                <.thead(
                  <.tr(
                    <.th("id"),
                    <.th("lastSeen"),
                    <.th())),
                <.tbody(
                  s.dangling.sortBy(_.id).map { dangling =>
                    Seq(
                      <.tr(
                        ^.onClick --> $.modState { _ =>
                          s.copy(expanded =
                            if (s.expanded.contains(dangling.id))
                              s.expanded - dangling.id
                            else
                              s.expanded + dangling.id) },
                        <.td(dangling.id.take(7)),
                        <.td(dangling.lastSeen),
                        <.td(Glyphicon(
                          if (s.expanded contains dangling.id)
                            "menu-down"
                          else
                            "menu-up")())
                      ),
                      <.tr(
                        <.td(
                          AppCSS.Style.hiddenTableRow,
                          ^.colSpan := 3,
                          if (s.expanded contains dangling.id)
                            danglingReservationDetails(dangling)
                          else
                            <.span())))
                  }
                ))
              }
          ))
      )
  }

  val DanglingReservationsComponent = ReactComponentB[Unit]("DanglingReservations").
    initialState(State(Nil)).
    renderBackend[Backend].
    componentDidMount(_.backend.start()).
    componentWillUnmount(_.backend.clear()).
    build

  def apply() = {
    DanglingReservationsComponent()
  }

}
