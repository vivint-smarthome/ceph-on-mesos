package org.vivint.ceph.lib


import scala.concurrent.{ ExecutionContext, Future }
object FutureHelpers {
  def tSequence[A,B,C](a: Future[A], b: Future[B], c: Future[C])(implicit ec: ExecutionContext): Future[(A,B,C)] = {
    a.flatMap { a => b.flatMap { b => c.map { c => (a,b,c) } } }
  }
  def tSequence[A,B,C,D](a: Future[A], b: Future[B], c: Future[C], d: Future[D])(implicit ec: ExecutionContext): Future[(A,B,C,D)] = {
    a.flatMap { a => b.flatMap { b => c.flatMap { c => d.map { d => (a,b,c,d) } } } }
  }
}
