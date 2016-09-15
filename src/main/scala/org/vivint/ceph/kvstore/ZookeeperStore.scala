package org.vivint.ceph.kvstore

import org.apache.curator.framework.CuratorFramework
import org.apache.zookeeper.KeeperException
import scala.concurrent.{ ExecutionContext, Future }
import scala.util.Try

class ZookeeperStore(client: CuratorFramework)(implicit ec: ExecutionContext) extends KVStore {

  def create(path: String, data: Array[Byte]): Future[Unit] = Future {
    client.create.
      creatingParentsIfNeeded.
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
    client.setData.inBackground.
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
}
