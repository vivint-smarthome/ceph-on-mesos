package org.vivint.ceph
package views

import akka.util.ByteString
import com.typesafe.config.ConfigObject
import java.net.{ Inet4Address, InetAddress }
import java.util.Base64
import model._
import org.apache.mesos.Protos
import ProtoHelpers._
import mesosphere.mesos.protos.Resource
import scala.collection.JavaConversions._
import scaldi.Injector
import scaldi.Injectable._
import configs.syntax._
import mesosphere.mesos.protos.Resource.PORTS
import java.nio.charset.StandardCharsets.UTF_8

object ConfigTemplates {
  private[views] def renderSettings(cfg: ConfigObject): String = {
    val b = new StringBuilder
    cfg.keySet.toSeq.sorted.foreach { k =>
      b.append(s"${k} = ${cfg(k).render}\n")
    }
    b.result
  }
}

class ConfigTemplates(implicit inj: Injector) {
  import ConfigTemplates._
  val config = inject[AppConfiguration]
  val resolver = inject[String => String]('ipResolver)

  def base64Encode(bs: ByteString): String = {
    Base64.getEncoder.encodeToString(bs.toArray)
  }

  // TODO - find a better home for these methods
  def inferPort(resources: Iterable[Protos.Resource], default: Int = 6789): Int =
    resources.
      toStream.
      filter(_.getName == PORTS).
      flatMap(_.ranges).
      headOption.
      map(_.min.toInt).
      getOrElse(default)
  def deriveLocation(offer: Protos.Offer): ServiceLocation = {
    val ip = resolver(offer.getHostname)

    val port = inferPort(offer.resources)

    ServiceLocation(
      offer.slaveId.get,
      offer.hostname.get,
      ip,
      port)
  }

  def cephConf(secrets: ClusterSecrets, monIps: Iterable[ServiceLocation],
    leaderOffer: Option[Protos.Offer], cephSettings: CephSettings) = {
    val monitors = leaderOffer.map(o => Iterable(deriveLocation(o))).getOrElse(monIps)
    s"""
[global]
fsid = ${secrets.fsid}
mon initial members = ${monitors.map(_.hostname).mkString(",")}
mon host = ${monitors.map { m => m.hostname + ":" + m.port }.mkString(",")}
mon addr =  ${monitors.map { m => m.ip + ":" + m.port }.mkString(",")}
auth cluster required = cephx
auth service required = cephx
auth client required = cephx
public network = ${config.publicNetwork}
cluster network = ${config.clusterNetwork}
${renderSettings(cephSettings.global)}

[auth]
${renderSettings(cephSettings.auth)}

[mon]
${renderSettings(cephSettings.mon)}

[osd]
${renderSettings(cephSettings.osd)}

[client]
${renderSettings(cephSettings.client)}

[mds]
${renderSettings(cephSettings.mds)}
"""
  }

  def cephClientAdminRing(secrets: ClusterSecrets) = {
    s"""
[client.admin]
	key = ${base64Encode(secrets.adminRing)}
	auid = 0
	caps mds = "allow"
	caps mon = "allow *"
	caps osd = "allow *"
"""
  }

  def cephMonRing(secrets: ClusterSecrets) = {
    s"""
[mon.]
	key = ${base64Encode(secrets.monRing)}
	caps mon = "allow *"
"""
  }

  def bootstrapMdsRing(secrets: ClusterSecrets) = {
    s"""
[client.bootstrap-mds]
	key = ${base64Encode(secrets.mdsRing)}
	caps mon = "allow profile bootstrap-mds"
"""
  }

  def bootstrapOsdRing(secrets: ClusterSecrets) = {
    s"""
[client.bootstrap-osd]
	key = ${base64Encode(secrets.osdRing)}
	caps mon = "allow profile bootstrap-osd"
"""
  }

  def bootstrapRgwRing(secrets: ClusterSecrets) = {
    s"""
[client.bootstrap-rgw]
	key = ${base64Encode(secrets.rgwRing)}
	caps mon = "allow profile bootstrap-rgw"
"""
  }

  def tgz(secrets: ClusterSecrets, monIps: Iterable[ServiceLocation],
    leaderOffer: Option[Protos.Offer], cephSettings: CephSettings): Array[Byte] = {
    import lib.TgzHelper.{octal, makeTgz, FileEntry}

    val entries = Seq(
      "etc/ceph/ceph.conf" -> cephConf(secrets, monIps, leaderOffer, cephSettings),
      "etc/ceph/ceph.client.admin.keyring" -> cephClientAdminRing(secrets),
      "etc/ceph/ceph.mon.keyring" -> cephMonRing(secrets),
      "var/lib/ceph/bootstrap-mds/ceph.keyring" -> bootstrapMdsRing(secrets),
      "var/lib/ceph/bootstrap-osd/ceph.keyring" -> bootstrapOsdRing(secrets),
      "var/lib/ceph/bootstrap-rgw/ceph.keyring" -> bootstrapRgwRing(secrets))

    makeTgz(entries.map { case (path, contents) =>
        path -> FileEntry(octal("644"), contents.getBytes(UTF_8))
    } : _*)
  }
}
