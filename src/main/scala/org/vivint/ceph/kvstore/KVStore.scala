package org.vivint.ceph.kvstore

import scala.concurrent.Future

trait KVStore {
  def create(path: String, data: Array[Byte]): Future[Unit]
  def set(path: String, data: Array[Byte]): Future[Unit]
  def createAndSet(path: String, data: Array[Byte]): Future[Unit]
  def delete(path: String): Future[Unit]
  def get(path: String): Future[Option[Array[Byte]]]
}
