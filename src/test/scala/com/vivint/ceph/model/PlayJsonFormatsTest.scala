package com.vivint.ceph.model

import org.scalatest.{FunSpec, Matchers}
import play.api.libs.json._

class PlayJsonFormatsTest extends FunSpec with Matchers {
  import PlayJsonFormats._
  describe("LocationFormat") {
    it("it goes in and out properly") {
      Json.toJson(Location.empty).as[Location] shouldBe Location.empty
      Json.toJson(PartialLocation(Some("ip"), None)).as[Location] shouldBe PartialLocation(Some("ip"), None)
      Json.toJson(PartialLocation(None, Some(1234))).as[Location] shouldBe PartialLocation(None, Some(1234))
      Json.toJson(IPLocation("ip", 1234)).as[Location] shouldBe IPLocation("ip", 1234)
      Json.toJson(ServiceLocation("hostname", "ip", 1234)).as[Location] shouldBe ServiceLocation("hostname", "ip", 1234)
    }
  }
}
