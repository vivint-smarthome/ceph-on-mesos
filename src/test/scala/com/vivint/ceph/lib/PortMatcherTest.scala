package com.vivint.ceph
package lib

import org.scalatest.{FunSpec, Matchers}
import mesosphere.mesos.protos.Resource.PORTS
import mesosphere.mesos.matcher.ResourceMatcher.ResourceSelector

class PortMatcherTest extends FunSpec with Matchers {
  import ProtoHelpers._
  describe("SpecificPortMatcher") {

    it("matches only the specified port") {
      val matcher = new SpecificPortMatcher(1000, ResourceSelector.any(Set("*")))
      val Seq(result) = matcher("offer-0", Seq(newRangesResource(PORTS, Seq(1000L to 1005L), role = "*")))

      result.matches shouldBe true
      val Seq(consumed) = result.consumedResources
      consumed.getRole shouldBe "*"
      consumed.getName shouldBe PORTS
      consumed.getRanges.getRange(0).getBegin shouldBe 1000L
      consumed.getRanges.getRange(0).getEnd shouldBe 1000L
    }

    it("matches ports in subsequent ranges") {
      val matcher = new SpecificPortMatcher(1000, ResourceSelector.any(Set("*")))
      val Seq(result) = matcher("offer-0",
        Seq(
          newRangesResource(PORTS, Seq(800L to 900L), role = "*"),
          newRangesResource(PORTS, Seq(1000L to 1005L), role = "*")))

      result.matches shouldBe true
      val Seq(consumed) = result.consumedResources
      consumed.getRole shouldBe "*"
      consumed.getName shouldBe PORTS
      consumed.getRanges.getRange(0).getBegin shouldBe 1000L
      consumed.getRanges.getRange(0).getEnd shouldBe 1000L
    }

    it("does not match if the specified port is not offered") {
      val matcher = new SpecificPortMatcher(999, ResourceSelector.any(Set("*")))
      val Seq(result) = matcher("offer-0", Seq(newRangesResource(PORTS, Seq(1000L to 1005L), role = "*")))

      result.matches shouldBe false
    }
  }

  describe("ContiguousPortMatcher") {
    it("matches a port block in subsequent ranges") {
      val matcher = new ContiguousPortMatcher(5, ResourceSelector.any(Set("*")))
      val Seq(result) = matcher("offer-0",
        Seq(
          newRangesResource(PORTS, Seq(800L to 801L), role = "*"),
          newRangesResource(PORTS, Seq(1000L to 1005L), role = "*")))

      result.matches shouldBe true
      val Seq(consumed) = result.consumedResources
      consumed.getRole shouldBe "*"
      consumed.getName shouldBe PORTS
      consumed.getRanges.getRange(0).getBegin shouldBe 1000L
      consumed.getRanges.getRange(0).getEnd shouldBe 1004L
    }

  }
}
