package org.vivint.ceph

import org.rogach.scallop._
import scala.concurrent.duration._

class CephFrameworkOptions(args: List[String]) extends ScallopConf(args) {
  val master = opt[String]("master", 'm',
    required = true,
    descr = "Mesos master location; in zk://ip1:port1,ip2:port2/mesos format")

  val name = opt[String]("name", 'n', descr = "framework name", default = Some("ceph"))

  val principal = opt[String]("principal", 'p',
    descr = "mesos principal as which to authenticate; can be set via MESOS_PRINCIPAL env var",
    required = true,
    default = Option(System.getenv("MESOS_PRINCIPAL")))

  val role = opt[String]("role", 'r',
    descr = "mesos role to use for reservations; can be set via MESOS_ROLE env var",
    default = Option(System.getenv("MESOS_ROLE")))

  val secret = opt[String]("secret", 's',
    descr = "mesos principal as which to authenticate; can be set via MESOS_SECRET env var",
    default = Option(System.getenv("MESOS_SECRET")))

  val zookeeper = opt[String]("zookeeper", 'z',
    required = true,
    descr = "Location for zookeeper for ceph-framework to store it's state")

  val publicNetwork = opt[String]("public-network",
    required = true,
    descr = "CIDR of the public network, in 0.0.0.0/24 format; can be set by PUBLIC_NETWORK",
    default = Option(System.getenv("PUBLIC_NETWORK")) )

  val clusterNetwork = opt[String]("cluster-network",
    descr = "CIDR of the ceph network, in 0.0.0.0/24 format; can be set by CEPH_NETWORK. Defaults to public-network.",
    default = Option(System.getenv("CLUSTER_NETWORK")) )

  val offerTimeout = opt[Int]("offer-timeout", 't',
    descr = "Duration in seconds after which offers timeout",
    required = false,
    default = Option(30))

  verify()
}
case class AppConfiguration(
  master: String,
  name: String,
  principal: String,
  secret: Option[String],
  role: String,
  zookeeper: String,
  offerTimeout: FiniteDuration,
  publicNetwork: String,
  clusterNetwork: String
)

object AppConfiguration {
  def fromArgs(args: List[String]): AppConfiguration = {
    val o = new CephFrameworkOptions(args)
    AppConfiguration(
      master = o.master(),
      name = o.name(),
      principal = o.principal(),
      secret = o.secret.toOption,
      role = o.role.toOption.
        orElse(o.principal.toOption).
        getOrElse(Constants.DefaultRole),
      zookeeper = o.zookeeper(),
      offerTimeout = o.offerTimeout().seconds,
      publicNetwork = o.publicNetwork(),
      clusterNetwork = o.clusterNetwork.toOption.getOrElse(o.publicNetwork()))
  }
}
