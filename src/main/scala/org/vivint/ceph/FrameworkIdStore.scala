package org.vivint.ceph

import org.vivint.ceph.kvstore.KVStore
import scala.concurrent.{ ExecutionContext, Future, Promise }
import org.apache.mesos.Protos
import scaldi.Injector
import scaldi.Injectable._

case class FrameworkIdStore(implicit injector: Injector) {
  private val kvStore = inject[KVStore]
  private val ksPath = "frameworkId"

  private val getP = Promise[Protos.FrameworkID]
  val get = getP.future

  import ExecutionContext.Implicits.global

  val initial = kvStore.get(ksPath).map { _.map { bytes =>
    Protos.FrameworkID.newBuilder().setValue(new String(bytes, "UTF-8")).build
  }}

  initial.foreach {
    case Some(fId) => getP.trySuccess(fId)
    case None => ()
  }


  /**
    * Synchronously updates the value. Throws on error.
    */
  def set(value: Protos.FrameworkID): Future[Unit] = {
    val bytes = value.getValue.getBytes("UTF-8")
    kvStore.set(ksPath, bytes).andThen { case _ =>
      getP.trySuccess(value)
    }
  }
}
