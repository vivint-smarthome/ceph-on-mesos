package com.vivint.ceph

import akka.stream.scaladsl.{ Sink, Source }
import org.slf4j.LoggerFactory
import akka.stream.scaladsl.Flow
import model.CephConfigHelper
import scala.concurrent.{ ExecutionContext, Future }

case class ConfigStore(kvStore: kvstore.KVStore) {
  private val log = LoggerFactory.getLogger(getClass)
  val configPath = "ceph.conf"

  def storeConfigIfNotExist(): Future[Unit] = {
    import ExecutionContext.Implicits.global

    kvStore.get(configPath).flatMap {
      case None =>
        kvStore.createOrSet(configPath, ConfigStore.default)
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

  def get = {
    kvStore.get(configPath).
      map {
        case Some(bytes) =>
          CephConfigHelper.parse(bytes)
        case None =>
          throw new RuntimeException("No configuration detected")
      }(ExecutionContext.global)
  }


    // .map(CephConfigHelper.parse)
}

object ConfigStore {
  lazy val default =  {
    import org.apache.commons.io.IOUtils
    val f = getClass.getResourceAsStream("/deployment-config.conf")
    try { IOUtils.toByteArray(f) }
    finally { f.close() }
  }
}
