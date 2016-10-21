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
import models.Job
import json._
import models.JsFormats._

object HomePage {

  object Style extends StyleSheet.Inline {
    import dsl._
    val content = style(textAlign.center,
      fontSize(30.px),
      minHeight(450.px),
      paddingTop(40.px))
  }


  val losItems = ReactComponentB[Seq[Job]]("Jobs").
    render_P { jobs =>
      <.div(
        jobs.map(_.id))
    }.
    build

  case class State(jobs: Seq[Job], expanded: Set[String] = Set.empty)

  import elements.{Table, Grid, Row, Col, Button}
  def renderLocation(jobId: String, location: models.Location): ReactNode = {
    val portSuffix = location.port.map(p => s":${p}").getOrElse("")

    (location.ip, location.hostname) match {
      case (Some(ip), Some(hostname)) =>
        elements.OverlayTrigger(
          overlay = elements.Tooltip(id = jobId + "-location")(ip),
          placement = "top")(<.div(s"${hostname}${portSuffix}"))
      case (Some(ip), None) =>
        <.span(s"${ip}${portSuffix}")
      case _ =>
        <.span("")
    }
  }

  val `data-toggle` = "data-toggle".reactAttr
  val `data-target` = "data-target".reactAttr

  class Backend($: BackendScope[Unit, State]) {
    private var running = true

    def poll(): Unit = {
      val xhr = new dom.XMLHttpRequest()
      xhr.open("GET", "/v1/jobs")
      xhr.onload = { (e: dom.Event) =>
        if (xhr.status == 200) {
          val jobs = JValue.fromString(xhr.responseText).toObject[Seq[Job]]
          $.modState { ste =>
            State(jobs, ste.expanded)
          }.runNow()
        } else {
          dom.console.log("error request job state", xhr.responseText)
        }

        import scala.concurrent.duration._
        js.timers.setTimeout(3.seconds) {
          if (running) poll()
        }
      }
      xhr.send()
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

    def setGoal(job: Job, state: String): Unit = {
      val xhr = new dom.XMLHttpRequest()
      xhr.open("PUT", s"/v1/jobs/${job.id}/${state}")
      xhr.onload = { (e: dom.Event) =>
        if (xhr.status == 200)
          dom.console.log(s"transition job ${job.id} to ${state} success")
        else
          dom.console.log(s"transition job ${job.id} to ${state} failed", xhr.responseText)
      }
      xhr.send()
    }


    def render(s: State) =
      <.div(
        s.jobs.groupBy { _.role }.toSeq.sortBy(_._1).map {
          case (role, roleJobs) =>
            <.div(
              <.h2(role),
              Table()(
                <.thead(
                  <.tr(
                    <.th("id"),
                    <.th("location"),
                    <.th("goal"),
                    <.th("state"))),
                <.tbody(
                  roleJobs.sortBy(_.id).flatMap { job =>
                    Seq(
                      <.tr(
                        ^.onClick --> $.modState { _ =>
                          s.copy(expanded =
                            if (s.expanded.contains(job.id)) s.expanded - job.id else s.expanded + job.id) },
                        <.td(job.id.take(7)),
                        <.td(renderLocation(job.id, job.location)),
                        <.td(job.goal.getOrElse[String]("")),
                        <.td(job.taskStatus.map(_.toString).getOrElse[String](""))),
                      <.tr(
                        <.td(
                          AppCSS.Style.hiddenTableRow,
                          ^.colSpan := 4,
                          if (s.expanded contains job.id)
                            Row()(
                              Col(
                                xs = 4)(
                                Table()(
                                  <.thead(
                                    <.tr(
                                      <.th("Field"),
                                      <.th("Value"))),
                                  <.tbody(
                                    <.tr(<.td("Behavior"), <.td(job.behavior)),
                                    <.tr(<.td("lastLaunched"), <.td(job.lastLaunched.getOrElse[String](""))),
                                    <.tr(<.td("goal"), <.td(job.goal.getOrElse[String](""))),
                                    <.tr(<.td("persistence"), <.td(s"${job.version} / ${job.persistentVersion}")),
                                    <.tr(<.td("wantingNewOffer"), <.td(job.wantingNewOffer.toString))))),
                              Col(
                                xs = 4)(
                                job.goal match {
                                  case Some("running") =>
                                    Button(bsStyle = "warning",
                                      onClick = { () => setGoal(job, "paused") })(
                                      "Pause")
                                  case Some("paused") =>
                                    Button(bsStyle = "success",
                                      onClick = { () => setGoal(job, "running") })(
                                      "Run")
                                  case _ =>
                                    <.span()
                                }
                              ))
                          else
                            <.span())))
                  }
                )
              )
            )

        }
      )
  }


  val JobsComponent = ReactComponentB[Unit]("Jobs").
    initialState(State(Nil)).
    renderBackend[Backend].
    componentDidMount(_.backend.start()).
    componentWillUnmount(_.backend.clear()).
    build

  def apply() = {
    JobsComponent()
  }

}
