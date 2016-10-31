package com.vivint.ceph.lib

import akka.actor.{ ActorContext, Kill }
import akka.event.LoggingAdapter
import scala.concurrent.{Future, ExecutionContext}
import scala.util.{Success,Failure}

object FutureMonitor {
  def logSuccess[T](log: LoggingAdapter, f: Future[T], desc: String)(implicit ex: ExecutionContext): Future[T] = {
    log.debug(s"${desc} : pulling state")
    f.onComplete {
      case Success(_) => log.debug("{} : success", desc)
      case Failure(ex) =>
        log.error(ex, "{}: failure", desc)
    }
    f
  }

  def crashSelfOnFailure(f: Future[Any], log: LoggingAdapter, description: String)(
    implicit context: ActorContext): Unit = {
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
