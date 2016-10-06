package com.vivint.ceph

import com.vivint.ceph.kvstore.KVStore
import scala.collection.immutable.Seq
import scala.concurrent.{ ExecutionContext, Future }
import scala.async.Async.{async, await}
import java.nio.charset.StandardCharsets.UTF_8
import play.api.libs.json._

case class TaskStore(kvStore: KVStore) {
  private val tasksPath = "tasks"
  import ExecutionContext.Implicits.global

  import model._

  import PlayJsonFormats._

  val nodePathRegex = s"^${NodeRole.values.mkString("|")}:".r
  private val parsingFunction: PartialFunction[String, (String, (JsValue => CephNode))] = {
    case path if nodePathRegex.findFirstMatchIn(path).nonEmpty =>
      (path, _.as[CephNode])
  }

  def getNodes: Future[Seq[CephNode]] = async {
    val paths = await(kvStore.children(tasksPath)).
      collect(parsingFunction).
      map { case (path, parser) =>
        (tasksPath + "/" + path, parser)
      }

    await(kvStore.getAll(paths.map(_._1))).
      zip(paths.map(_._2)).
      map { case (optBytes, parser) =>
        optBytes.map { bytes =>
          (parser(Json.parse(bytes)))
        }
      }.
      flatten
  }

  def save(node: CephNode): Future[Unit] = {
    val data = Json.toJson(node).toString
    kvStore.createAndSet(s"tasks/${node.role}:" + node.id.toString, data.getBytes(UTF_8))
  }
}
