package org.vivint.ceph
package views

import java.net.{ Inet4Address, InetAddress }
import model._
import org.apache.mesos.Protos
import ProtoHelpers._
import mesosphere.mesos.protos.Resource
import scaldi.Injector
import scaldi.Injectable._

class ConfigTemplates(secrets: ClusterSecrets, monIps: List[ServiceLocation])(implicit inj: Injector) {
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

  def cephConf(leaderOffer: Option[Protos.Offer], deploymentConfig: DeploymentConfig) = {
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
osd journal size = ${deploymentConfig.osd.journalSize}
"""

  }

  // def
}
