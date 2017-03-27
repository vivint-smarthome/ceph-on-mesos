package com.vivint.ceph

import com.vivint.ceph.model.CephConfigHelper
import org.scalatest.{FunSpec, Matchers}

class ConfigStoreTest extends FunSpec with Matchers {
  describe("default config") {
    it("provides default values based on the present of environment variables") {
      val cfgString = ConfigStore.default(
        Map(
          "CEPH_MON_INIT_PORT" -> "999"))
      // println(new String(cfgString))
      val cfg = CephConfigHelper.parse(cfgString)
      cfg.deployment.mon.port.shouldBe(Some(999))
    }
  }
}
