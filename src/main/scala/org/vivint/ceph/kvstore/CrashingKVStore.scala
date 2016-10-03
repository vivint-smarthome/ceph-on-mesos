package org.vivint.ceph.kvstore

import akka.actor.{ ActorContext, Kill }
import akka.stream.scaladsl.Source
import org.slf4j.LoggerFactory
import scala.collection.immutable.Seq
import scala.concurrent.Future

class CrashingKVStore(kvStore: KVStore)(implicit context: ActorContext)
    extends KVStore {

  val log = LoggerFactory.getLogger(getClass)
  private def wrap[T](f: Future[T]): Future[T] = {
    f.onFailure { case ex =>
      log.error(s"KVStore exception in ${context.self.path}", ex)
      context.self ! Kill
    }(context.dispatcher)
    f
  }

  override def getAll(paths: Seq[String]): Future[Seq[Option[Array[Byte]]]] = wrap(kvStore.getAll(paths))
  def create(path: String, data: Array[Byte]): Future[Unit] = wrap(kvStore.create(path, data))
  def set(path: String, data: Array[Byte]): Future[Unit] = wrap(kvStore.set(path, data))
  def createAndSet(path: String, data: Array[Byte]): Future[Unit] = wrap(kvStore.createAndSet(path, data))
  def delete(path: String): Future[Unit] = wrap(kvStore.delete(path))
  def get(path: String): Future[Option[Array[Byte]]] = wrap(kvStore.get(path))
  def children(path: String): Future[Seq[String]] = wrap(kvStore.children(path))
  def watch(path: String, bufferSize: Int = 1): Source[Option[Array[Byte]], KVStore.CancellableWithResult] =
    kvStore.watch(path).mapMaterializedValue { r =>
      wrap(r.result)
      r
    }
}

object CrashingKVStore {
  def apply(kvStore: KVStore)(implicit context: ActorContext): CrashingKVStore =
    new CrashingKVStore(kvStore)
}
