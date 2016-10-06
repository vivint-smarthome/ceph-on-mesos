package org.vivint.ceph.lib

import org.scalatest.{FunSpec,Matchers}
import java.nio.charset.StandardCharsets.UTF_8

class TgzHelperTest extends FunSpec with Matchers {
  it("should tar and untar a file") {
    import TgzHelper._
    val tgz = makeTgz(
      "path/to/file" -> FileEntry(octal("777"), "hello".getBytes(UTF_8)),
      "path/to/file2" -> FileEntry(octal("777"), "bye".getBytes(UTF_8))
    )
    readTgz(tgz).map { case (k,v) => (k, new String(v, UTF_8)) }.toMap shouldBe Map(
      "path/to/file" -> "hello",
      "path/to/file2" -> "bye")
  }
}
