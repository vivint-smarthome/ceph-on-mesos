package com.vivint.ceph

import java.util.UUID
import org.slf4j.LoggerFactory
import scala.async.Async.{async, await}
import scala.collection.immutable.Seq
import scala.concurrent.{ExecutionContext, Future}
import java.nio.charset.StandardCharsets.UTF_8

import com.vivint.ceph.kvstore.KVStore
import model._
import play.api.libs.json._

case class ReleaseStore(kvStore: KVStore) {
  import PlayJsonFormats._
  import ExecutionContext.Implicits.global
  val log = LoggerFactory.getLogger(getClass)

  val releasesPath = "reservation_releases"
  def getReleases: Future[Seq[ReservationRelease]] = async {
    val paths = await(kvStore.children(releasesPath)).
      map { path =>
        (releasesPath + "/" + path)
      }

    val result = await(kvStore.getAll(paths)).
      flatten.
      map { bytes => Json.parse(bytes).as[ReservationRelease] }
    log.debug("loaded {} reservation releases", result.length)
    result
  }

  def save(release: ReservationRelease): Future[Unit] = {
    log.debug("saving release for reservation {}", release.id)
    val data = Json.toJson(release).toString
    kvStore.createOrSet(s"${releasesPath}/${release.id}", data.getBytes(UTF_8))
  }

  def delete(reservationId: UUID): Future[Unit] = {
    log.debug("deleting release for reservation {}", reservationId)
    kvStore.delete(s"${releasesPath}/${reservationId}")
  }
}
