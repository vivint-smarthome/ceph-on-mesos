package com.vivint.ceph

import com.vivint.ceph.kvstore.KVStore
import scala.concurrent.{ ExecutionContext, Future }
import java.nio.charset.StandardCharsets.UTF_8
import model.ClusterSecrets
import model.PlayJsonFormats._
import play.api.libs.json._

// We should probably encrypt this
object ClusterSecretStore {
  def createOrGenerateSecrets(kvStore: KVStore): Future[ClusterSecrets] = {
    import ExecutionContext.Implicits.global

    kvStore.get("secrets.json").flatMap {
      case Some(secrets) =>
        Future.successful(
          Json.parse(secrets).as[ClusterSecrets])
      case None =>
        val secrets = ClusterSecrets.generate
        kvStore.set("secrets.json", Json.toJson(secrets).toString().getBytes(UTF_8)).map { case _ =>
          secrets }
    }
  }
}
