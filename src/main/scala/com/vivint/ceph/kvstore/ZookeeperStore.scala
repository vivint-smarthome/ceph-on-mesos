package com.vivint.ceph.kvstore

import akka.Done
import java.util.concurrent.{ Executors, TimeUnit }
import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.state.{ ConnectionState, ConnectionStateListener }
import org.apache.zookeeper.KeeperException.ConnectionLossException

import scala.collection.JavaConversions._
import scala.collection.immutable.Seq
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.Try

import akka.stream.OverflowStrategy
import akka.stream.scaladsl.{Source, SourceQueueWithComplete}
import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.framework.recipes.cache.{NodeCache, NodeCacheListener}
import org.apache.curator.framework.recipes.locks.InterProcessSemaphoreMutex
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
    namespace(namespace).
    retryPolicy(retryPolicy).
    build()
  client.start()

  val executor = Executors.newSingleThreadExecutor()
  implicit private val ec = ExecutionContext.fromExecutor(executor)

  Runtime.getRuntime.addShutdownHook(new Thread {
    override def run(): Unit = {
      System.err.println("Draining ZK writes")
      executor.shutdown()
      executor.awaitTermination(1, TimeUnit.MINUTES)
    }
  })

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

  def lock(path: String): Future[KVStore.CancellableWithResult] = Future {
    val lock = new InterProcessSemaphoreMutex(client, path)
    lock.acquire()
    val p = Promise[Done]

    val listener = new ConnectionStateListener {
      def stateChanged(client: CuratorFramework, newState: ConnectionState): Unit = {
        if (!newState.isConnected()) {
          p.failure(new ConnectionLossException)
        }
      }
    }
    p.future.onComplete { _ =>
      client.getConnectionStateListenable.removeListener(listener)
      lock.release()
    }

    new KVStore.CancellableWithResult {
      def result = p.future
      def cancel(): Boolean = { p.trySuccess(Done) }
      def isCancelled = p.future.isCompleted
    }
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
