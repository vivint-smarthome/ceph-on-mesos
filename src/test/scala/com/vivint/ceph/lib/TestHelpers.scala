package com.vivint.ceph.lib

import scala.concurrent.{ Await, Awaitable }
import scala.concurrent.duration._


trait TestHelpers {
  def await[T](f: Awaitable[T], duration: FiniteDuration = 5.seconds) = {
    Await.result(f, duration)
  }

}
