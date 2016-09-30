package org.vivint.ceph.model

import akka.util.ByteString
import java.security.SecureRandom
import java.util.UUID

case class ClusterSecrets(
  fsid: UUID,
  adminRing: ByteString,
  monRing: ByteString,
  mdsRing: ByteString,
  osdRing: ByteString,
  rgwRing: ByteString)

object ClusterSecrets {
  val KeySize = 28
  def generateKey = {
    val random = SecureRandom.getInstanceStrong
    val bytes = Array.fill[Byte](28)(0)
    random.nextBytes(bytes)
    ByteString(bytes)
  }

  def generate: ClusterSecrets = {
    ClusterSecrets(
      UUID.randomUUID,
      generateKey,
      generateKey,
      generateKey,
      generateKey,
      generateKey)
  }
}
