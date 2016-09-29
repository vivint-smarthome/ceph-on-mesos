package org.vivint.ceph.kvstore

import akka.actor.{ Actor, ActorLogging, Cancellable }
import akka.stream.scaladsl.SourceQueueWithComplete
import org.apache.curator.framework.recipes.cache.NodeCacheListener
import org.apache.curator.framework.{ CuratorFramework, CuratorFrameworkFactory }
import org.apache.curator.framework.recipes.cache.NodeCache
import org.apache.zookeeper.KeeperException
import org.vivint.ceph.AppConfiguration
import scala.concurrent.{ ExecutionContext, Future, Promise }
import scala.collection.JavaConversions._
import scala.util.Try
import scaldi.Injector
import scaldi.Injectable._

/** By serializing all requests in and out of Zookeeper we get linear consistency guarantees. IE an an async read
  * request that follows an async write is guaranteed to get the state of the async write */
class ZookeeperActor(implicit injector: Injector) extends Actor with ActorLogging {
  val framework = inject[AppConfiguration]
  import org.apache.curator.retry.ExponentialBackoffRetry
  private val retryPolicy = new ExponentialBackoffRetry(1000, 3)
  import ZookeeperActor._


  val appConfiguration = inject[AppConfiguration]
  val client = CuratorFrameworkFactory.builder.
    connectString(appConfiguration.zookeeper).
    namespace("ceph-on-mesos").
    retryPolicy(retryPolicy).
    build()

  log.info("Starting Zookeeper actor")

  def receive = {
    case (p: Promise[_], o: Operation) =>
      p.asInstanceOf[Promise[o.Response]].complete(Try(o(client)))
  }
}

object ZookeeperActor {
  sealed trait Operation { self =>
    type Response
    private[kvstore] def apply(c: CuratorFramework): self.Response
  }

  case class Create(path: String, data: Array[Byte]) extends Operation { self =>
    type Response = Unit
    private[kvstore] def apply(client: CuratorFramework): self.Response = {
      client.create.
        creatingParentsIfNeeded.
        forPath(path, data)
    }
  }

  case class Set(path: String, data: Array[Byte]) extends Operation { self =>
    type Response = Unit
    private[kvstore] def apply(client: CuratorFramework): self.Response = {
      client.setData.
        forPath(path, data)
    }
  }

  case class CreateAndSet(path: String, data: Array[Byte]) extends Operation { self =>
    type Response = Unit
    private[kvstore] def apply(client: CuratorFramework): self.Response = {
      Try {
        client.create.
          creatingParentsIfNeeded.
          forPath(path)
      }
      client.setData.
        forPath(path, data)
    }
  }

  case class Delete(path: String) extends Operation { self =>
    type Response = Unit
    private[kvstore] def apply(client: CuratorFramework): self.Response = {
      client.delete.forPath(path)
    }
  }

  case class Get(path: String) extends Operation { self =>
    type Response = Option[Array[Byte]]
    private[kvstore] def apply(client: CuratorFramework): self.Response = {
      try Some(client.getData.forPath(path))
      catch {
        case _: KeeperException.NoNodeException =>
          None
      }
    }
  }

  case class Children(path: String) extends Operation { self =>
    type Response = Seq[String]
    private[kvstore] def apply(client: CuratorFramework): self.Response = {
      client.getChildren.forPath(path).toSeq
    }
  }

  case class WireSourceQueue(path: String, queue: SourceQueueWithComplete[Option[Array[Byte]]]) extends Operation { self =>
    type Response = Unit
    private[kvstore] def apply(client: CuratorFramework): self.Response = {
      var _isCancelled = false
      val l = new NodeCache(client, path)
      l.getListenable.addListener(new NodeCacheListener {
        override def nodeChanged(): Unit = {
          queue.offer(Option(l.getCurrentData.getData))
        }
      })
      l.start()

      queue.watchCompletion().onComplete { _ =>
        l.close()
        _isCancelled = true
      }(ExecutionContext.global)
    }
  }
}

/*
  /** Returns an Akka stream Source which emits the state of the Zookeeper node. A new listener is created for each
    * materialization of this source. On buffer overflow (due to backpressure), old values are dropped to make room for
    * new ones. The listener is closed when the stream is stopped.
    */
  def subscription(path: String, bufferSize: Int = 1): Source[Option[ByteString], Cancellable] = {
    Source.queue[Option[ByteString]](bufferSize, OverflowStrategy.dropHead).
      mapMaterializedValue { q =>
        var _isCancelled = false
        val l = new NodeCache(namespaceFramework, path)
        l.getListenable.addListener(new NodeCacheListener {
          override def nodeChanged(): Unit = {
            val s = Option(l.getCurrentData).map { d => ByteString(d.getData) }
            q.offer(s)
          }
        })
        l.start()

        q.watchCompletion().onComplete { _ =>
          l.close()
          _isCancelled = true
        }(ExecutionContext.global)

        new Cancellable {
          def cancel(): Boolean = { q.complete(); true }
          def isCancelled = _isCancelled
        }
      }
  }
 */
