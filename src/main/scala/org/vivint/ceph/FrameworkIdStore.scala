package org.vivint.ceph

import org.vivint.ceph.kvstore.KVStore
import scala.concurrent.{ ExecutionContext, Future }
import org.apache.mesos.Protos

case class FrameworkIdStore(kvStore: KVStore) {
  private val ksPath = "frameworkId"

  def get: Future[Option[Protos.FrameworkID]] = {
    kvStore.get(ksPath).map { _.map { bytes =>
      Protos.FrameworkID.newBuilder().setValue(new String(bytes, "UTF-8")).build
    }}(ExecutionContext.global)
  }

  /**
    * Synchronously updates the value. Throws on error.
    */
  def set(value: Protos.FrameworkID): Future[Unit] = {
    val bytes = value.getValue.getBytes("UTF-8")
    kvStore.set(ksPath, bytes)
  }
}
