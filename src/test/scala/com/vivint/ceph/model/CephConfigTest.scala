package com.vivint.ceph.model

import org.apache.commons.io.IOUtils
import org.scalatest.{FunSpec, Matchers}
import scala.collection.JavaConversions._
import java.nio.charset.StandardCharsets.UTF_8
import mesosphere.marathon.state.DiskType

class CephConfigTest extends FunSpec with Matchers {
  val exampleFile = IOUtils.toString(getClass.getResourceAsStream("/deployment-config.conf"), UTF_8)
  describe("parsing") {
    it("should be able to parse the example file") {
      val config = CephConfigHelper.parse(exampleFile)

      config.deployment.mon.count shouldBe 0
      config.deployment.mon.cpus shouldBe 1.0
      config.deployment.mon.mem shouldBe 256.0
      config.deployment.mon.disk_type shouldBe (DiskType.Root)
      config.deployment.mon.disk shouldBe (16)

      config.deployment.osd.count shouldBe 0
      config.deployment.osd.cpus shouldBe 1.0
      config.deployment.osd.mem shouldBe 1024.0
      config.deployment.osd.disk_type shouldBe (DiskType.Mount)
      config.deployment.osd.disk shouldBe (512000)
      config.deployment.osd.disk_max shouldBe (None)
      config.deployment.osd.path_constraint shouldBe (None)

      config.settings.auth.keySet.toSet shouldBe Set(
        "cephx_service_require_signatures", "cephx", "cephx_cluster_require_signatures",
        "cephx_require_signatures")
    }
  }
}
