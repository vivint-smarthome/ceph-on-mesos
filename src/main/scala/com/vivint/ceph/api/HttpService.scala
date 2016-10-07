package com.vivint.ceph
package api

import akka.actor.{ ActorRef, ActorSystem }
import akka.http.scaladsl.model.ParsingException
import akka.http.scaladsl.server.ExceptionHandler
import akka.util.Timeout
import akka.pattern.ask
import akka.http.scaladsl.Http
import akka.stream.ActorMaterializer
import com.vivint.ceph.model.RunState
import java.util.UUID
import scaldi.Injector
import scaldi.Injectable._
import scala.concurrent.duration._
import akka.http.scaladsl.server.Directives._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

class HttpService(implicit inj: Injector) {
  implicit val actorSystem = inject[ActorSystem]
  implicit val materializer = ActorMaterializer()
  import ApiMarshalling._

  val config = inject[AppConfiguration]
  val taskActor = inject[ActorRef](classOf[TaskActor])

  val a = null

  implicit val timeout = Timeout(5.seconds)


  implicit def myExceptionHandler: ExceptionHandler =
    ExceptionHandler {
      case ex: ParsingException =>
        complete((400, s"Error parsing: ${ex.getMessage}"))
    }

  def getTasks: Future[Map[String, model.Task]] =
    (taskActor ? TaskActor.GetTasks).mapTo[Map[String, model.Task]]

  def findTaskByUUID(id: UUID) =
    getTasks.map { _.values.find(_.id == id) }

  def route = pathPrefix("v1") {
    pathPrefix("tasks") {
      (pathEnd & get) {
        onSuccess(getTasks) { tasks =>
          complete(tasks.values.toList)
        }
      } ~
      (put & path(Segment.map(uuidFromString) / Segment.map(runStateFromString))) { (id, runState) =>
        onSuccess(findTaskByUUID(id)) {
          case Some(task) =>
            val message = TaskActor.UpdateGoal(task.taskId, runState)
            taskActor ! message
            complete(s"Task ID ${task.taskId} state change submitted: ${task.goal} -> ${Some(runState)}")
          case None =>
            complete((400, s"Couldn't find task with UUID ${id}."))
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

