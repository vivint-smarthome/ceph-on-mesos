package org.vivint.ceph.kvstore

import akka.Done
import akka.actor.Cancellable
import akka.stream.scaladsl.Source
import scala.concurrent.{ ExecutionContext, Future }

/** KVStore trait. Asynchronous but implementations MUST PROVIDE linearized read/write guarantees; IE, if two
  * overlapping async writes are fired off in the same thread, the second write request must always win.
  */
trait KVStore {
  def create(path: String, data: Array[Byte]): Future[Unit]
  def set(path: String, data: Array[Byte]): Future[Unit]
  def createAndSet(path: String, data: Array[Byte]): Future[Unit]
  def delete(path: String): Future[Unit]
  def get(path: String): Future[Option[Array[Byte]]]

  def getAll(paths: Seq[String]): Future[Seq[Option[Array[Byte]]]] = {
    import ExecutionContext.Implicits.global
    Future.sequence {
      paths.map(get)
    }
  }

  def children(path: String): Future[Seq[String]]

  def watch(path: String, bufferSize: Int = 1): Source[Option[Array[Byte]], KVStore.CancellableWithResult]
}

object KVStore {
  trait CancellableWithResult extends Cancellable {
    def result: Future[Done]
  }
}
