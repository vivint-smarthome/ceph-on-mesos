package com.vivint.ceph
package api

import akka.actor.{ ActorRef, ActorSystem }
import akka.util.Timeout
import akka.pattern.ask
import akka.http.scaladsl.Http
import akka.stream.ActorMaterializer
import scaldi.Injector
import scaldi.Injectable._
import scala.concurrent.duration._
import akka.http.scaladsl.server.Directives._
import scala.concurrent.ExecutionContext.Implicits.global
import model.NodeState

class HttpService(implicit inj: Injector) {
  implicit val actorSystem = inject[ActorSystem]
  implicit val materializer = ActorMaterializer()
  import model.PlayJsonFormats._
  import ApiMarshalling._

  val config = inject[AppConfiguration]
  val taskActor = inject[ActorRef](classOf[TaskActor])

  implicit val timeout = Timeout(5.seconds)
  def route = pathPrefix("v1") {
    pathPrefix("tasks") {
      (pathEnd & get) {
        onSuccess((taskActor ? TaskActor.GetTasks).mapTo[Map[String, model.NodeState]]) { tasks =>
          complete(tasks.values.toList)
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

