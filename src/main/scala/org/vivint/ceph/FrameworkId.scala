package org.vivint.ceph

import org.vivint.ceph.kvstore.KVStore
import scala.concurrent.{ Await, Future }
import scaldi.Injectable._
import scaldi.Injector
import scala.concurrent.duration._
import org.apache.mesos.Protos

class FrameworkId(implicit val injector: Injector) {
  private val ksPath = "frameworkId"
  private lazy val kvStore = inject[KVStore]

  def get: Option[Protos.FrameworkID] = {
    Await.result(kvStore.get(ksPath), 30.seconds).map { bytes =>
      Protos.FrameworkID.newBuilder().setValue(new String(bytes, "UTF-8")).build
    }
  }

  /**
    * Synchronously updates the value. Throws on error.
    */
  def set(value: Protos.FrameworkID): Unit = {
    val bytes = value.getValue.getBytes("UTF-8")
    Await.result(kvStore.set(ksPath, bytes), 30.seconds)
  }
}
