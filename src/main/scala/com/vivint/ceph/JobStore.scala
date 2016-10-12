package com.vivint.ceph

import com.vivint.ceph.kvstore.KVStore
import scala.collection.immutable.Seq
import scala.concurrent.{ ExecutionContext, Future }
import scala.async.Async.{async, await}
import java.nio.charset.StandardCharsets.UTF_8
import play.api.libs.json._

case class JobStore(kvStore: KVStore) {
  private val tasksPath = "tasks"
  import ExecutionContext.Implicits.global

  import model._

  import PlayJsonFormats._

  def getTasks: Future[Seq[PersistentState]] = async {
    val paths = await(kvStore.children(tasksPath)).
      map { path =>
        (tasksPath + "/" + path)
      }

    await(kvStore.getAll(paths)).
      zip(paths).
      map {
        case (Some(bytes), _) =>
          (Json.parse(bytes).as[PersistentState])
        case (None, path) =>
          throw new RuntimeException(s"Error: empty task state at path ${path}")
      }
  }

  def save(task: PersistentState): Future[Unit] = {
    val data = Json.toJson(task).toString
    kvStore.createOrSet(s"tasks/${task.role}:" + task.id.toString, data.getBytes(UTF_8))
  }
}
