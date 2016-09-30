package org.vivint.ceph.model

import org.apache.commons.io.IOUtils
import org.scalatest.{FunSpec, Matchers}
import java.nio.charset.StandardCharsets.UTF_8

class DeploymentConfigTest extends FunSpec with Matchers {
  val exampleFile = IOUtils.toString(getClass.getResourceAsStream("deployment-config.conf"), UTF_8)
  describe("parsing") {
    it("should be able to parse the example file") {
      println(DeploymentConfigHelper.parse(exampleFile))
    }
  }

}
