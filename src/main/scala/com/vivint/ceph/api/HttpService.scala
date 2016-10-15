package com.vivint.ceph
package api

import akka.actor.{ ActorRef, ActorSystem }
import akka.http.scaladsl.model.{ ContentTypes, MediaTypes }
import akka.http.scaladsl.model.headers.`Content-Type`
import akka.http.scaladsl.model.{ HttpHeader, ParsingException }
import akka.http.scaladsl.server.ExceptionHandler
import akka.util.Timeout
import akka.pattern.ask
import akka.http.scaladsl.Http
import akka.stream.ActorMaterializer
import com.vivint.ceph.kvstore.KVStore
import com.vivint.ceph.views.ConfigTemplates
import scala.collection.breakOut
import com.vivint.ceph.model.{ RunState, ServiceLocation, JobRole }
import java.util.UUID
import scaldi.Injector
import scaldi.Injectable._
import scala.concurrent.duration._
import akka.http.scaladsl.server.Directives._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import lib.FutureHelpers.tSequence
import lib.TgzHelper.makeTgz

class HttpService(implicit inj: Injector) {
  implicit val actorSystem = inject[ActorSystem]
  implicit val materializer = ActorMaterializer()
  import ApiMarshalling._

  val config = inject[AppConfiguration]
  val taskActor = inject[ActorRef](classOf[TaskActor])
  val configTemplates = inject[ConfigTemplates]

  val a = null

  implicit val timeout = Timeout(5.seconds)
  val kvStore = inject[KVStore]

  implicit def myExceptionHandler: ExceptionHandler =
    ExceptionHandler {
      case ex: ParsingException =>
        complete((400, s"Error parsing: ${ex.getMessage}"))
    }

  def getJobs: Future[Map[UUID, model.Job]] =
    (taskActor ? TaskActor.GetJobs).mapTo[Map[UUID, model.Job]]

  def findJobByUUID(id: UUID) =
    getJobs.map { _.values.find(_.id == id) }

  def getConfig: Future[String] = {
    tSequence(
      ClusterSecretStore.createOrGenerateSecrets(kvStore),
      ConfigStore(kvStore).get,
      getJobs).map {
      case (secrets, cfg, jobs) =>
        val monitors: Set[ServiceLocation] =
          jobs.values.filter(_.role == JobRole.Monitor).flatMap(_.pState.serviceLocation)(breakOut)
        configTemplates.cephConf(secrets, monitors, cfg.settings, None)
    }
  }

  def route = pathPrefix("v1") {
    // TODO - protect with key
    path("config" / "ceph.conf") {
      complete(getConfig)
    } ~
    pathPrefix("jobs") {
      (pathEnd & get) {
        onSuccess(getJobs) { jobs =>
          complete(jobs.values.toList)
        }
      } ~
      (put & path(Segment.map(uuidFromString) / Segment.map(runStateFromString))) { (id, runState) =>
        onSuccess(findJobByUUID(id)) {
          case Some(job) =>
            taskActor ! TaskActor.UpdateGoal(job.id, runState)
            complete(s"Job ID ${job.id} state change submitted: ${job.goal} -> ${Some(runState)}")
          case None =>
            complete((400, s"Couldn't find job with UUID ${id}."))
        }
      }
    }
  }

  def run() = {
    Http().bindAndHandle(
      route,
      config.apiHost,
      config.apiPort)
  }
}

