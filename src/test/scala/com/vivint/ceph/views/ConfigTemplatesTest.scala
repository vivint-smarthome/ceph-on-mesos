package com.vivint.ceph.views

import org.scalatest.{FunSpec, Matchers}
import com.typesafe.config.{ConfigFactory,ConfigObject}
import configs.syntax._

class ConfigTemplatesTest extends FunSpec with Matchers {
  val config = ConfigFactory.parseString("""
auth {
  number = 1
  double = 1.5
  boolean = false
  string = "very string"
}
""")

  describe("renderingSettings") {
    it("renders various keys of a config object") {
      val cObj = config.get[ConfigObject]("auth").value
      ConfigTemplates.renderSettings(cObj) shouldBe (
        """boolean = false
          |double = 1.5
          |number = 1
          |string = "very string"
          |""".stripMargin)
    }
  }
}
