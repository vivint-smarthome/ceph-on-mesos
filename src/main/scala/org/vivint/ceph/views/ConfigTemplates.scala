package org.vivint.ceph
package views

import com.typesafe.config.ConfigObject
import java.net.{ Inet4Address, InetAddress }
import model._
import org.apache.mesos.Protos
import ProtoHelpers._
import mesosphere.mesos.protos.Resource
import scala.collection.JavaConversions._
import scaldi.Injector
import scaldi.Injectable._
import configs.syntax._

object ConfigTemplates {
  private[views] def renderSettings(cfg: ConfigObject): String = {
    val b = new StringBuilder
    cfg.keySet.toSeq.sorted.foreach { k =>
      b.append(s"${k} = ${cfg(k).render}\n")
    }
    b.result
  }
}

class ConfigTemplates(secrets: ClusterSecrets, monIps: List[ServiceLocation])(implicit inj: Injector) {
  import ConfigTemplates._
  val config = inject[AppConfiguration]
  val resolver = inject[String => String]('ipResolver)

  def deriveLocation(offer: Protos.Offer): ServiceLocation = {
    val ip = resolver(offer.getHostname)

    val port = offer.resources.toStream.
      filter(_.getName == Resource.PORTS).
      flatMap { _.ranges.headOption.map(_.min.toInt) }.
      headOption

    ServiceLocation(
      offer.slaveId.get,
      offer.hostname.get,
      ip,
      port.getOrElse(6789))
  }

  def cephConf(leaderOffer: Option[Protos.Offer], cephSettings: CephSettings) = {
    val monitors = leaderOffer.map(o => List(deriveLocation(o))).getOrElse(monIps)
    s"""
[global]
fsid = ${secrets.fsid}
mon initial members = ${monitors.map(_.hostname).mkString(",")}
mon host = ${monitors.map(_.hostname).mkString(",")}
mon addr =  ${monitors.map { m => m.ip + ":" + m.port }.mkString(",")}}
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
}
