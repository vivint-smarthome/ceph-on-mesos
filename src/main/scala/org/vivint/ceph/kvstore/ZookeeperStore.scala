package org.vivint.ceph.kvstore

import akka.actor.ActorRef
import akka.stream.OverflowStrategy
import akka.stream.scaladsl.Source
import scala.collection.immutable.Seq
import scala.concurrent.{ ExecutionContext, Future, Promise }

import scaldi.Injector
import scaldi.Injectable._

/** Uses ZookeeperActor store to give linearized read / write guarantees */
class ZookeeperStore(implicit injector: Injector) extends KVStore {
  val zkActor = inject[ActorRef](classOf[ZookeeperActor])
  import ZookeeperActor._
  private def request[T <: ZookeeperActor.Operation](cmd: T): Future[T#Response] = {
    val p = Promise[cmd.Response]
    zkActor ! ((p, cmd))
    p.future
  }

  def create(path: String, data: Array[Byte]): Future[Unit] =
    request(Create(path, data))

  def set(path: String, data: Array[Byte]): Future[Unit] =
    request(Set(path, data))

  def createAndSet(path: String, data: Array[Byte]): Future[Unit] =
    request(CreateAndSet(path, data))

  def delete(path: String): Future[Unit] =
    request(Delete(path))

  def get(path: String): Future[Option[Array[Byte]]] =
    request(Get(path))

  def children(path: String): Future[Seq[String]] =
    request(Children(path))

  def watch(path: String, bufferSize: Int = 1): Source[Option[Array[Byte]], KVStore.CancellableWithResult] =
    Source.queue[Option[Array[Byte]]](bufferSize, OverflowStrategy.dropHead).mapMaterializedValue { queue =>
      var _isCancelled = false
      val result = request(WireSourceQueue(path, queue))
      result.onFailure { case ex =>
        queue.fail(ex)
      }(ExecutionContext.global)

      new KVStore.CancellableWithResult {
        def result = queue.watchCompletion()
        def cancel(): Boolean = { queue.complete(); true }
        def isCancelled = _isCancelled
      }
    }
}
