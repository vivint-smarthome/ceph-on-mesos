package org.vivint.ceph

import scala.concurrent.ExecutionContext

object SameThreadExecutionContext extends ExecutionContext {
  def execute(r: Runnable): Unit =
    r.run()
  override def reportFailure(t: Throwable): Unit =
    throw new IllegalStateException("problem in internal callback", t)
}
