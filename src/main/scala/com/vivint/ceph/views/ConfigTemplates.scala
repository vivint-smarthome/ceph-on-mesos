package com.vivint.ceph
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
import scala.collection.immutable.NumericRange
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

  def base64Encode(bs: ByteString): String = {
    Base64.getEncoder.encodeToString(bs.toArray)
  }

  def cephConf(secrets: ClusterSecrets, monitors: Set[ServiceLocation], cephSettings: CephSettings,
    osdPort: Option[NumericRange.Inclusive[Long]]) = {

    val sortedMonitors = monitors.toSeq.sortBy(_.hostname)

    val osdPortSection = osdPort.map { port =>
      s"""
      |## ports
      |ms_bind_port_min = ${port.min}
      |ms_bind_port_max = ${port.max}
      |""".stripMargin
    }.getOrElse("")

    s"""
[global]
fsid = ${secrets.fsid}
mon initial members = ${sortedMonitors.map(_.hostname).mkString(",")}
mon host = ${sortedMonitors.map { m => m.hostname + ":" + m.port }.mkString(",")}
mon addr = ${sortedMonitors.map { m => m.ip + ":" + m.port }.mkString(",")}
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
${osdPortSection}

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

  def tgz(secrets: ClusterSecrets, monitors: Set[ServiceLocation], cephSettings: CephSettings,
    osdPort: Option[NumericRange.Inclusive[Long]] = None): Array[Byte] = {
    import lib.TgzHelper.{octal, makeTgz, FileEntry}

    val entries = Seq(
      "etc/ceph/ceph.conf" -> cephConf(secrets, monitors, cephSettings, osdPort),
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
