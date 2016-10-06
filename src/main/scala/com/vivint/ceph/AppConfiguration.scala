package com.vivint.ceph

import com.typesafe.config.ConfigFactory
import org.rogach.scallop._
import scala.concurrent.duration._
import AppConfiguration.config

class CephFrameworkOptions(args: List[String]) extends ScallopConf(args) {
  val master = opt[String]("master", 'm',
    required = true,
    descr = "Mesos master location; in zk://ip1:port1,ip2:port2/mesos format")

  val name = opt[String]("name", 'n', descr = "framework name", default = Some("ceph"))

  val principal = opt[String]("principal", 'p',
    descr = "mesos principal as which to authenticate; can be set via MESOS_PRINCIPAL env var. Default = ceph.",
    required = true,
    default = Option(System.getenv("MESOS_PRINCIPAL")).orElse(Some("ceph")))

  val role = opt[String]("role", 'r',
    descr = "mesos role to use for reservations; can be set via MESOS_ROLE env var. Default = ceph.",
    default = Option(System.getenv("MESOS_ROLE")).orElse(Some("ceph")))

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
    descr = "CIDR of the ceph network, in 0.0.0.0/24 format; can be set by CEPH_NETWORK. Default = <public-network>.",
    default = Option(System.getenv("CLUSTER_NETWORK")).orElse(publicNetwork.toOption))

  val offerTimeout = opt[Int]("offer-timeout", 't',
    descr = "Duration in seconds after which offers timeout; Default = 30.",
    required = false,
    default = Option(30))

  val storageBackend = opt[String]("storage-backend", 'b',
    descr = "KV storage backend. Options: file, memory, or zookeeper. Default = zookeeper.",
    required = false,
    default = Some("zookeeper"))

  val failoverTimeout = opt[Long]("failover-timeout",
    descr = "Duration in seconds after which to timeout the framework (stopping all tasks); Default = 31536000 (1 year)",
    required = false,
    default = Some(31536000))

  val apiPort = opt[Int]("api-port",
    descr = s"HTTP API port; can be set via API_PORT; default 8080",
    default = Option(System.getenv("API_PORT")).map(_.toInt)).orElse(Some(8080))

  val apiHost = opt[String]("api-host",
    descr = s"HTTP API host; can be set via API_HOST; default 127.0.0.1",
    default = Option(System.getenv("API_HOST"))).orElse(Some("127.0.0.1"))

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
  clusterNetwork: String,
  storageBackend: String,
  failoverTimeout: Long = 31536000L,
  apiPort: Int = config.getInt("api.port"),
  apiHost: String = config.getString("api.host")
) {
  require(AppConfiguration.validStorageBackends.contains(storageBackend))
}

object AppConfiguration {
  val config = ConfigFactory.load

  val validStorageBackends = Set("zookeeper", "file", "memory")
  def fromOpts(o: CephFrameworkOptions): AppConfiguration = {
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
      clusterNetwork = o.clusterNetwork(),
      storageBackend = o.storageBackend(),
      failoverTimeout = o.failoverTimeout(),
      apiPort = o.apiPort(),
      apiHost = o.apiHost()
    )
  }
}
