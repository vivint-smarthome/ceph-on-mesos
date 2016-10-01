package org.vivint.ceph

import org.slf4j.LoggerFactory
import akka.stream.scaladsl.Flow
import model.CephConfigHelper
import java.nio.charset.StandardCharsets.UTF_8
import scala.concurrent.{ ExecutionContext, Future }

case class ConfigStore(kvStore: kvstore.KVStore) {
  private val log = LoggerFactory.getLogger(getClass)
  val configPath = "config.json"

  def storeConfigIfNotExist(): Future[Unit] = {
    import ExecutionContext.Implicits.global

    kvStore.get(configPath).flatMap {
      case None =>
        import org.apache.commons.io.IOUtils
        val byteArray = Array.empty[Byte]
        IOUtils.readFully(
          getClass.getResourceAsStream("/deployment-config.conf"),
          byteArray)
        kvStore.set(configPath, byteArray)
      case Some(_) =>
        Future.successful(())
    }
  }

  val configParsingFlow = Flow[Option[Array[Byte]]].
    map {
      case Some(bytes) =>
        try Some(CephConfigHelper.parse(new String(bytes, UTF_8)))
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
}
