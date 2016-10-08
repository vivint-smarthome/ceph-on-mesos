package com.vivint.ceph.lib

import akka.actor.{ ActorContext, Kill }
import akka.event.LoggingAdapter
import scala.concurrent.Future

object FutureMonitor {
  def monitor(f: Future[Any], log: LoggingAdapter, description: String)(implicit context: ActorContext): Unit = {
    var failed = false
    if (failed) // prevent infinite loops if all children actors get restarted
      context.stop(context.self)
    else
      f.onFailure {
        case ex =>
          failed = true
          context.self ! Kill
          log.error(ex, s"Unexpected error for ${description}")
      }(context.dispatcher)
  }
}
