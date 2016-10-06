package com.vivint.ceph
package kvstore

import akka.actor.{ ActorContext, Props, Actor, ActorRef }
import akka.stream.scaladsl.Source
import scala.collection.immutable.Seq
import scala.concurrent.{ Future, Promise }
import scala.util.Failure

/** KVStore which forwards the first failure through the crashed channel.
  * If this fails then the CrashingKVStore should be reinitialized
  */
case class CrashingKVStore(kvStore: KVStore) extends KVStore {
  private [this] val p = Promise[Unit]
  val crashed: Future[Unit] = p.future

  private def wrap[T](f: () => Future[T]): Future[T] = {
    if (crashed.isCompleted) {
      // throw the future in the calling thread to make it obvious that this shouldn't be used any more
      val Failure(ex) = crashed.value.get
      throw new IllegalStateException("This kvstore has crashed and should not be used any more", ex)
    }
    val resultingFuture = f()

    resultingFuture.onFailure { case ex =>
      p.tryFailure(ex)
    }(SameThreadExecutionContext)
    resultingFuture
  }

  override def getAll(paths: Seq[String]): Future[Seq[Option[Array[Byte]]]] = wrap(() => kvStore.getAll(paths))
  def create(path: String, data: Array[Byte]): Future[Unit] = wrap(() => kvStore.create(path, data))
  def set(path: String, data: Array[Byte]): Future[Unit] = wrap(() => kvStore.set(path, data))
  def createAndSet(path: String, data: Array[Byte]): Future[Unit] = wrap(() => kvStore.createAndSet(path, data))
  def delete(path: String): Future[Unit] = wrap(() => kvStore.delete(path))
  def get(path: String): Future[Option[Array[Byte]]] = wrap(() => kvStore.get(path))
  def children(path: String): Future[Seq[String]] = wrap(() => kvStore.children(path))
  def watch(path: String, bufferSize: Int = 1): Source[Option[Array[Byte]], KVStore.CancellableWithResult] =
    kvStore.watch(path).mapMaterializedValue { r =>
      wrap(() => r.result)
      r
    }
}
