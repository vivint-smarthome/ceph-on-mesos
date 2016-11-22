package com.vivint.ceph.model

import akka.util.ByteString
import java.security.SecureRandom
import java.util.UUID
import java.nio.{ByteOrder,ByteBuffer}
case class ClusterSecrets(
  fsid: UUID,
  adminRing: ByteString,
  monRing: ByteString,
  mdsRing: ByteString,
  osdRing: ByteString,
  rgwRing: ByteString)

object ClusterSecrets {
  lazy val random = new SecureRandom()
  val KeySize = 16
  def generateKey = {
    // 0000000: 0100 3c64 f357 3dfe bd34 1000 2a00 93c7  ..<d.W=..4..*...
    // 0000010: 057f 89b6 c0b0 4682 6d22 33f1            ......F.m"3.
    val b = ByteBuffer.allocate(2 + 8 + 2 + KeySize)
    b.order(ByteOrder.LITTLE_ENDIAN)
    b.putShort(1)
    b.putInt((System.currentTimeMillis / 1000).toInt)
    b.putInt((System.nanoTime).toInt)
    b.putShort(16)
    val bytes = Array.fill[Byte](KeySize)(0)
    random.nextBytes(bytes)
    b.put(bytes)
    ByteString(b.array)
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
