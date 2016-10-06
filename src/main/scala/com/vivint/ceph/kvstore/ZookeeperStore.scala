package com.vivint.ceph.kvstore

import java.util.concurrent.Executors

import scala.collection.JavaConversions._
import scala.collection.immutable.Seq
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try

import akka.stream.OverflowStrategy
import akka.stream.scaladsl.{Source, SourceQueueWithComplete}
import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.framework.recipes.cache.{NodeCache, NodeCacheListener}
import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.zookeeper.KeeperException
import com.vivint.ceph.AppConfiguration
import scaldi.Injectable._
import scaldi.Injector

/** Uses ZookeeperActor store to give linearized read / write guarantees */
class ZookeeperStore(namespace: String = "ceph-on-mesos")(implicit injector: Injector) extends KVStore {
  private val retryPolicy = new ExponentialBackoffRetry(1000, 3)
  val appConfiguration = inject[AppConfiguration]
  val client = CuratorFrameworkFactory.builder.
    connectString(appConfiguration.zookeeper).
    namespace("ceph-on-mesos").
    retryPolicy(retryPolicy).
    build()

  implicit private val ec = ExecutionContext.fromExecutor(
    Executors.newSingleThreadExecutor())

  def create(path: String, data: Array[Byte]): Future[Unit] = Future {
    client.create.
      creatingParentsIfNeeded.
      forPath(path, data)
    client.setData.
      forPath(path, data)
  }

  def set(path: String, data: Array[Byte]): Future[Unit] = Future {
    client.setData.
      forPath(path, data)
  }

  def createAndSet(path: String, data: Array[Byte]): Future[Unit] = Future {
    Try {
      client.create.
        creatingParentsIfNeeded.
        forPath(path)
    }
    client.setData.
      forPath(path, data)
  }

  def delete(path: String): Future[Unit] = Future {
    client.delete.forPath(path)
  }

  def get(path: String): Future[Option[Array[Byte]]] = Future {
    try Some(client.getData.forPath(path))
    catch {
      case _: KeeperException.NoNodeException =>
        None
    }
  }

  def children(path: String): Future[Seq[String]] = Future {
    client.getChildren.forPath(path).toList
  }

  private def wireSourceQueue(path: String, queue: SourceQueueWithComplete[Option[Array[Byte]]]): Future[Unit] = Future {
    var _isCancelled = false
    val l = new NodeCache(client, path)
    l.getListenable.addListener(new NodeCacheListener {
      override def nodeChanged(): Unit = {
        queue.offer(Option(l.getCurrentData.getData))
      }
    })
    l.start()

    queue.watchCompletion().onComplete { _ =>

      _isCancelled = true
    }(ExecutionContext.global)
  }

  def watch(path: String, bufferSize: Int = 1): Source[Option[Array[Byte]], KVStore.CancellableWithResult] =
    Source.queue[Option[Array[Byte]]](bufferSize, OverflowStrategy.dropHead).mapMaterializedValue { queue =>
      var _isCancelled = false
      val result = wireSourceQueue(path, queue)
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
