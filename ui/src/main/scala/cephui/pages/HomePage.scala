package cephui
package pages

import cephui.css.AppCSS
import japgolly.scalajs.react._
import japgolly.scalajs.react.extra.Reusability
import japgolly.scalajs.react.vdom.prefix_<^._
import scalacss.Defaults._
import scala.scalajs.js
import scalacss.ScalaCssReact._

object HomePage {

  object Style extends StyleSheet.Inline {
    import dsl._
    val content = style(textAlign.center,
      fontSize(30.px),
      minHeight(450.px),
      paddingTop(40.px))
  }

  val dataJson = s"""
[
  {
    "id": "48c4492b-4b9a-477f-896e-e5090ade4513",
    "cluster": "ceph",
    "role": "mon",
    "goal": "running",
    "lastLaunched": "running",
    "reservationConfirmed": true,
    "reservationId": "4246a627-cf0b-4604-8771-2d2c5e07d803",
    "slaveId": "8adeadbf-1032-4f04-972f-e439933b77ff-S2",
    "taskId": "ceph.mon.094ba994-ea7a-4f97-bf86-e41e0f1e8634",
    "location": {
      "hostname": "mesos-2.sw.domain",
      "ip": "172.0.0.12",
      "port": 31002
    },
    "version": 2,
    "persistentVersion": 2,
    "behavior": "Running",
    "wantingNewOffer": false,
    "taskStatus": "TASK_RUNNING"
  },
  {
    "id": "d5654498-16f9-4552-ba1a-b7192dd2a83b",
    "cluster": "ceph",
    "role": "osd",
    "goal": "running",
    "lastLaunched": "running",
    "reservationConfirmed": true,
    "reservationId": "6e0fe0ae-c17f-420c-bed4-f381b252afe7",
    "slaveId": "8adeadbf-1032-4f04-972f-e439933b77ff-S5",
    "taskId": "ceph.osd.cc39562e-c93a-4216-9164-b5dc9f966404",
    "location": {
      "hostname": "mesos-6.sw.domain",
      "ip": "172.0.0.10",
      "port": 31002
    },
    "version": 2,
    "persistentVersion": 2,
    "behavior": "Running",
    "wantingNewOffer": false,
    "taskStatus": "TASK_RUNNING"
  },
  {
    "id": "96522943-2d55-450b-b4ac-e58c80164cc0",
    "cluster": "ceph",
    "role": "osd",
    "goal": "running",
    "lastLaunched": "running",
    "reservationConfirmed": true,
    "reservationId": "d704bd12-66b8-442d-adf0-2b9653896f1c",
    "slaveId": "8adeadbf-1032-4f04-972f-e439933b77ff-S8",
    "taskId": "ceph.osd.4d86098d-a677-4a31-8259-0471c30e17df",
    "location": {
      "hostname": "mesos-4.sw.domain",
      "ip": "172.0.0.14",
      "port": 31003
    },
    "version": 2,
    "persistentVersion": 2,
    "behavior": "Running",
    "wantingNewOffer": false,
    "taskStatus": "TASK_RUNNING"
  },
  {
    "id": "302840dd-199e-4654-a9d5-2b8f07d338b5",
    "cluster": "ceph",
    "role": "rgw",
    "goal": "running",
    "lastLaunched": "running",
    "reservationConfirmed": false,
    "slaveId": "8adeadbf-1032-4f04-972f-e439933b77ff-S8",
    "taskId": "ceph.rgw.b5d23bac-0cc5-4044-bc26-6642fd4b594f",
    "location": {
      "ip": null,
      "port": 80
    },
    "version": 4,
    "persistentVersion": 4,
    "behavior": "EphemeralRunning",
    "wantingNewOffer": false,
    "taskStatus": "TASK_RUNNING"
  },
  {
    "id": "c3a32c8d-0481-4bae-b244-7f156c237c99",
    "cluster": "ceph",
    "role": "osd",
    "goal": "running",
    "lastLaunched": "running",
    "reservationConfirmed": true,
    "reservationId": "651b764d-e0d9-4eb1-a60a-0f8eaa954e3c",
    "slaveId": "8adeadbf-1032-4f04-972f-e439933b77ff-S6",
    "taskId": "ceph.osd.d9fdc6cd-7645-4340-860a-d6619c22623a",
    "location": {
      "hostname": "mesos-1.sw.domain",
      "ip": "172.0.0.11",
      "port": 31003
    },
    "version": 2,
    "persistentVersion": 2,
    "behavior": "Running",
    "wantingNewOffer": false,
    "taskStatus": "TASK_RUNNING"
  },
  {
    "id": "e77a9541-0d8d-43d1-ac9a-37e740cf7a3b",
    "cluster": "ceph",
    "role": "mon",
    "goal": "running",
    "lastLaunched": "running",
    "reservationConfirmed": true,
    "reservationId": "cd75b9d6-596c-4e9b-ad2f-8a4bc2bbb5d5",
    "slaveId": "8adeadbf-1032-4f04-972f-e439933b77ff-S8",
    "taskId": "ceph.mon.824e86d2-b966-411c-ad5e-730ddecfebc6",
    "location": {
      "hostname": "mesos-4.sw.domain",
      "ip": "172.0.0.14",
      "port": 31002
    },
    "version": 2,
    "persistentVersion": 2,
    "behavior": "Running",
    "wantingNewOffer": false,
    "taskStatus": "TASK_RUNNING"
  },
  {
    "id": "e973a30e-2233-41b9-8a50-ee4dab0825c7",
    "cluster": "ceph",
    "role": "osd",
    "goal": "running",
    "lastLaunched": "running",
    "reservationConfirmed": true,
    "reservationId": "52967553-93e7-4d08-acb3-48eb0a97770a",
    "slaveId": "8adeadbf-1032-4f04-972f-e439933b77ff-S7",
    "taskId": "ceph.osd.0443a572-3a1d-4002-8c73-d34b0895ac2b",
    "location": {
      "hostname": "mesos-5.sw.domain",
      "ip": "172.0.0.15",
      "port": 31002
    },
    "version": 2,
    "persistentVersion": 2,
    "behavior": "Running",
    "wantingNewOffer": false,
    "taskStatus": "TASK_RUNNING"
  },
  {
    "id": "404ad65c-3a4e-4f26-a6d3-9659d7c16b22",
    "cluster": "ceph",
    "role": "osd",
    "goal": "running",
    "lastLaunched": "running",
    "reservationConfirmed": true,
    "reservationId": "67f183f4-6561-4333-811a-9367dc8e3780",
    "slaveId": "8adeadbf-1032-4f04-972f-e439933b77ff-S9",
    "taskId": "ceph.osd.c937577e-4992-4497-bb72-fca077112b76",
    "location": {
      "hostname": "mesos-3.sw.domain",
      "ip": "172.0.0.13",
      "port": 31000
    },
    "version": 2,
    "persistentVersion": 2,
    "behavior": "Running",
    "wantingNewOffer": false,
    "taskStatus": "TASK_RUNNING"
  },
  {
    "id": "6fa83b0e-509b-44aa-8276-181ed33a59fb",
    "cluster": "ceph",
    "role": "rgw",
    "goal": "running",
    "lastLaunched": "running",
    "reservationConfirmed": false,
    "slaveId": "8adeadbf-1032-4f04-972f-e439933b77ff-S7",
    "taskId": "ceph.rgw.a1f22dca-f34b-430b-b759-bc6334e4871a",
    "location": {
      "ip": null,
      "port": 80
    },
    "version": 4,
    "persistentVersion": 4,
    "behavior": "EphemeralRunning",
    "wantingNewOffer": false,
    "taskStatus": "TASK_RUNNING"
  },
  {
    "id": "15ba7541-8306-4db2-8865-931e79be6738",
    "cluster": "ceph",
    "role": "rgw",
    "goal": "running",
    "lastLaunched": "running",
    "reservationConfirmed": false,
    "slaveId": "8adeadbf-1032-4f04-972f-e439933b77ff-S6",
    "taskId": "ceph.rgw.3c1999d7-66b7-4ff4-b962-c2cc28c15e5f",
    "location": {
      "ip": null,
      "port": 80
    },
    "version": 4,
    "persistentVersion": 4,
    "behavior": "EphemeralRunning",
    "wantingNewOffer": false,
    "taskStatus": "TASK_RUNNING"
  },
  {
    "id": "f7befdc1-828e-485f-ad50-f68898a0d51f",
    "cluster": "ceph",
    "role": "osd",
    "goal": "running",
    "lastLaunched": "running",
    "reservationConfirmed": true,
    "reservationId": "ae62106c-2811-43d6-ad16-61c57f3c3ef9",
    "slaveId": "8adeadbf-1032-4f04-972f-e439933b77ff-S2",
    "taskId": "ceph.osd.7834fce8-4369-4491-8ad6-af3241bd7aab",
    "location": {
      "hostname": "mesos-2.sw.domain",
      "ip": "172.0.0.12",
      "port": 31003
    },
    "version": 2,
    "persistentVersion": 2,
    "behavior": "Running",
    "wantingNewOffer": false,
    "taskStatus": "TASK_RUNNING"
  },
  {
    "id": "26c41c59-d76f-40a1-b446-5bf431ec9f0f",
    "cluster": "ceph",
    "role": "mon",
    "goal": "running",
    "lastLaunched": "running",
    "reservationConfirmed": true,
    "reservationId": "7c90af24-d1f3-407a-abab-60204dd7aa6f",
    "slaveId": "8adeadbf-1032-4f04-972f-e439933b77ff-S6",
    "taskId": "ceph.mon.790356fc-3219-407f-b60e-708112d01eef",
    "location": {
      "hostname": "mesos-1.sw.domain",
      "ip": "172.0.0.11",
      "port": 31002
    },
    "version": 2,
    "persistentVersion": 2,
    "behavior": "Running",
    "wantingNewOffer": false,
    "taskStatus": "TASK_RUNNING"
  }
]
"""

  import json._
  import models.JsFormats._

  val jobs = JValue.fromString(dataJson).toObject[Seq[models.Job]]

  org.scalajs.dom.window.console.log(jobs.toString)

  val losItems = ReactComponentB[Seq[models.Job]]("Jobs").
    render_P { jobs =>
      <.div(
        jobs.map(_.id))
    }.
    build

  // val component = ReactComponentB.static("HomePage",
  //   <.div(Style.content, "ScalaJS-React Template ")
  // ).buildU

  // def apply(jobs: Seq[models.Job],
  //   ref: js.UndefOr[String] = "", key: js.Any = {}) = losItems.set(key, ref)(data)

  case class State(jobs: Seq[models.Job], expanded: Set[String] = Set.empty)

  import elements.{Table, Grid, Row, Accordion, Panel, Col, Button}
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
                                    Button(bsStyle = "warning")("Pause")
                                  case Some("paused") =>
                                    Button(bsStyle = "success")("Run")
                                  case None =>
                                    <.span()
                                }
                              ))
                          else
                            <.span())))
                  }
                )
              )
              // Accordion()(
              //   roleJobs.sortBy(_.id).map { job =>
              //     val header: ReactNode =
              //       Grid()(
              //         Row()(
              //           Col(md = 1)(s"id: ${job.id.take(7)}"),
              //           Col(md = 1)(renderLocation(job.id, job.location))
              //         ))

              //     Panel(header = header, key = job.id, eventKey = job.id)(
              //       job.slaveId.getOrElse[String](""))
              //   }
              // )
            )

        }
      )
  }

  val JobsComponent = ReactComponentB[Unit]("Jobs")
    .initialState(State(Nil))
    .renderBackend[Backend]
    .componentDidMount { scope => scope.modState(_ => State(jobs)) }
    .build

  def apply() = {
    JobsComponent()
  }

}
