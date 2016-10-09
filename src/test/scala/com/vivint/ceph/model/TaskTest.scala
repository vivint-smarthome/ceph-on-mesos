package com.vivint.ceph.model

import org.scalatest.{FunSpec, Matchers}

class TaskTest extends FunSpec with Matchers {
  describe("TaskStatus") {
    it("converts to and form mesos") {
      val original = TaskStatus("abc", "123", TaskState.TaskRunning)
      TaskStatus.fromMesos(original.toMesos) shouldBe original
    }
  }
}
