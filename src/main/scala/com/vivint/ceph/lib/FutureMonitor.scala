package com.vivint.ceph.lib

import akka.actor.{ Actor, ActorContext, ActorLogging, Props }
import scala.concurrent.Future
import scala.util.{Success,Failure}


object FutureMonitor {
  def monitor(f: Future[Any], description: String)(implicit context: ActorContext) = {
    var failed = false
    context.actorOf(Props(new Actor with ActorLogging {
      if (failed) // prevent infinite loops if all children actors get restarted
        context.stop(self)
      else
        f.onComplete {
          case Success(_) =>
            context.stop(self)
          case Failure(ex) =>
            failed = true
            log.error(ex, s"Unexpected error for ${description}")
            self ! ex
      }(context.dispatcher)
      def receive = { case ex: Throwable => throw(ex) }
    }))
  }
}
