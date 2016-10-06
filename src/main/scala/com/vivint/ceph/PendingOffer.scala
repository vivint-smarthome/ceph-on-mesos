package com.vivint.ceph

import org.apache.mesos.Protos.Offer
import scala.concurrent.Promise
import scala.collection.immutable.Seq

case class PendingOffer(offer: Offer,
  resultingOperationsPromise: Promise[Seq[Offer.Operation]] = Promise[Seq[Offer.Operation]]) {
  val resultingOperations = resultingOperationsPromise.future

  def slaveId = offer.getSlaveId.getValue
}
