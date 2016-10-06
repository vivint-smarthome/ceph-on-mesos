package com.vivint.ceph.kvstore


import akka.stream.OverflowStrategy
import akka.stream.scaladsl.SourceQueueWithComplete
import java.io.File
import java.util.concurrent.Executors
import scala.collection.immutable.Seq
import scala.concurrent.{ ExecutionContext, Future }
import akka.stream.scaladsl.Source

/** For use in tests.
  */
class MemStore extends KVStore {

  implicit private val ec = ExecutionContext.fromExecutor(
    Executors.newSingleThreadExecutor())

  private var paths = Set.empty[File]
  private var state = Map.empty[File, Array[Byte]]
  private var subscriptions = Set.empty[SourceQueueWithComplete[File]]

  private def createFolders(path: File): Unit = {
    val parent = path.getParentFile
    if (parent != null) {
      paths = paths + parent
      createFolders(parent)
    }
  }

  private [kvstore] def removeSubscription(queue: SourceQueueWithComplete[File]) = Future {
    subscriptions = subscriptions - queue
  }

  private [kvstore] def addSubscription(queue: SourceQueueWithComplete[File]) = Future {
    subscriptions = subscriptions + queue
  }

  def fileFor(path: String) =
    new File("/", path)

  def create(path: String, data: Array[Byte]): Future[Unit] = Future {
    val output = fileFor(path)
    if (state.contains(output))
      throw new RuntimeException(s"path ${path} already exists")
    createFolders(output)
    state = state.updated(output, data)
    subscriptions.foreach(_.offer(output))
  }

  def set(path: String, data: Array[Byte]): Future[Unit] = Future {
    val output = fileFor(path)
    if (! paths.contains(output.getParentFile))
      throw new RuntimeException(s"no such parent for ${output}: ${output.getParentFile}")
    state = state.updated(output, data)
    subscriptions.foreach(_.offer(output))
  }

  def createAndSet(path: String, data: Array[Byte]): Future[Unit] = Future {
    val output = fileFor(path)
    createFolders(output)
    state = state.updated(output, data)
    subscriptions.foreach(_.offer(output))
  }

  def delete(path: String): Future[Unit] = Future {
    val deleteFile = fileFor(path)
    state = state - deleteFile
    subscriptions.foreach(_.offer(deleteFile))
  }

  def get(path: String): Future[Option[Array[Byte]]] = Future {
    val input = fileFor(path)
    state.get(input)
  }
  private [kvstore] def get(path: File): Future[Option[Array[Byte]]] = Future {
    state.get(path)
  }


  def children(path: String): Future[Seq[String]] = Future {
    val parent = fileFor(path)
    state.keys.
      filter { _.getParentFile == parent }.
      map ( _.getName ).
      toList
  }

  def watch(path: String, bufferSize: Int = 1): Source[Option[Array[Byte]], KVStore.CancellableWithResult] = {
    val input = fileFor(path)

    Source.queue[File](bufferSize, OverflowStrategy.dropHead).
      mapMaterializedValue { queue =>
        addSubscription(queue).onComplete { _ => queue.offer(input) }

        var _isCancelled = false
        new KVStore.CancellableWithResult {
          def result = queue.watchCompletion()
          def cancel(): Boolean = { queue.complete(); true }
          def isCancelled = _isCancelled
        }
      }.
      filter(_ == input).
      mapAsync(1)(get)
  }
}
