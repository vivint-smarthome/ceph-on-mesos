package com.vivint.ceph

import akka.stream.scaladsl.Flow
import java.nio.charset.StandardCharsets.UTF_8
import model.CephConfigHelper
import org.slf4j.LoggerFactory
import scala.concurrent.{ ExecutionContext, Future }

case class ConfigStore(kvStore: kvstore.KVStore) {
  private val log = LoggerFactory.getLogger(getClass)
  val configPath = "ceph.conf"

  def storeConfigIfNotExist(): Future[Unit] = {
    import ExecutionContext.Implicits.global

    kvStore.get(configPath).flatMap {
      case None =>
        kvStore.createOrSet(configPath, ConfigStore.default())
      case Some(_) =>
        Future.successful(())
    }
  }

  val configParsingFlow = Flow[Option[Array[Byte]]].
    map {
      case Some(bytes) =>
        try Some(CephConfigHelper.parse(bytes))
        catch { case ex: Throwable =>
          log.error("Error parsing configuration", ex)
          None
        }
      case None =>
        log.error("No configuration detected.")
        None
    }
  def stream =
    kvStore.watch(configPath).
      via(configParsingFlow)

  def getText =
    kvStore.get(configPath)

  def storeText(str: String) = {
    try {
      model.CephConfigHelper.parse(str)
      kvStore.set(configPath, str.getBytes(UTF_8))
    } catch {
      case ex: Throwable =>
        Future.failed(ex)
    }
  }

  def get = {
    getText.
      map {
        case Some(bytes) =>
          CephConfigHelper.parse(bytes)
        case None =>
          throw new RuntimeException("No configuration detected")
      }(ExecutionContext.global)
  }
}

object ConfigStore {
  def default(environment: Map[String, String] = sys.env): Array[Byte] = {
    val setMonInitFixedPort: String => String = { input =>
      environment.get("CEPH_MON_INIT_PORT") match {
        case Some(port) =>
          input.replace("# port = 2015", s"port = ${port}")
        case None =>
          input
      }
    }

    import org.apache.commons.io.IOUtils
    val f = getClass.getResourceAsStream("/deployment-config.conf")
    try {
      val defaultTemplate = new String(IOUtils.toByteArray(f))
      setMonInitFixedPort(defaultTemplate).
        getBytes(UTF_8)
    }
    finally { f.close() }
  }
}
