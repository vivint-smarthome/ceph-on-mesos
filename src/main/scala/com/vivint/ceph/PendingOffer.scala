package com.vivint.ceph

import org.apache.mesos.Protos.Offer
import scala.concurrent.Promise
import scala.collection.immutable.Seq

case class PendingOffer(offer: Offer) {
  private val resultingOperationsPromise = Promise[Seq[Offer.Operation]]
  def decline(): Boolean = respond(Nil)
  def respond(ops: Seq[Offer.Operation]): Boolean =
    resultingOperationsPromise.trySuccess(ops)
  def responded: Boolean =
    resultingOperationsPromise.isCompleted

  val resultingOperations = resultingOperationsPromise.future

  def slaveId = offer.getSlaveId.getValue
}
