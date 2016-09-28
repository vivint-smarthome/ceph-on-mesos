package org.vivint.ceph.kvstore

import scala.concurrent.{ ExecutionContext, Future }

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
}
