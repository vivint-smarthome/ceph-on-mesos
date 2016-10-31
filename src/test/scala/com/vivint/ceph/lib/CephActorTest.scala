package com.vivint.ceph.lib

import akka.actor.ActorSystem
import akka.testkit.TestKit
import java.util.concurrent.atomic.AtomicInteger
import org.scalatest.BeforeAndAfterAll
import org.scalatest.fixture.Suite


abstract class CephActorTest(name: String) extends TestKit(ActorSystem(name)) with Suite with BeforeAndAfterAll {
  val idx = new AtomicInteger()
  override def afterAll(): Unit = {
    super.afterAll()
    TestKit.shutdownActorSystem(system)
  }
}
