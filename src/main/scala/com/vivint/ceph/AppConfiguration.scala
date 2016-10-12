package com.vivint.ceph

import org.rogach.scallop._
import scala.concurrent.duration._

class CephFrameworkOptions(args: List[String]) extends ScallopConf(args) {

  def env(key: String) = Option(System.getenv(key))

  val master = opt[String]("master", 'm',
    required = true,
    descr = "Mesos master location; in zk://ip1:port1,ip2:port2/mesos format. Can set via MESOS_MASTER",
    default = env("MESOS_MASTER"))

  val name = opt[String]("name",
    descr = "framework name; can set via FRAMEWORK_NAME. Default = ceph",
    default = env("FRAMEWORK_NAME").orElse(Some("ceph")))

  val principal = opt[String]("principal",
    descr = "mesos principal as which to authenticate; can set via MESOS_PRINCIPAL. Default = ceph.",
    required = true,
    default = env("MESOS_PRINCIPAL").orElse(Some("ceph")))

  val role = opt[String]("role",
    descr = "mesos role to use for reservations; can set via MESOS_ROLE. Default = ceph.",
    default = env("MESOS_ROLE").orElse(Some("ceph")))

  val secret = opt[String]("secret",
    descr = "mesos principal as which to authenticate; can set via MESOS_SECRET.",
    default = env("MESOS_SECRET"))

  val zookeeper = opt[String]("zookeeper",
    required = true,
    descr = "Location for zookeeper for ceph-framework to store it's state. Don't prefix with zk://. Can set via ZOOKEEPER",
    default = env("ZOOKEEPER"))

  val publicNetwork = opt[String]("public-network",
    required = true,
    descr = "CIDR of the public network, in 0.0.0.0/24 format; can be set by PUBLIC_NETWORK",
    default = env("PUBLIC_NETWORK") )

  val clusterNetwork = opt[String]("cluster-network",
    descr = "CIDR of the ceph network, in 0.0.0.0/24 format; can be set by CEPH_NETWORK. Default = <public-network>.",
    default = env("CLUSTER_NETWORK"))

  val offerTimeout = opt[Int]("offer-timeout",
    descr = "Duration in seconds after which offers timeout; Default = 30.",
    required = false,
    default = env("OFFER_TIMEOUT").map(_.toInt).orElse(Some(30)))

  val storageBackend = opt[String]("storage-backend",
    descr = "KV storage backend. Options: file, memory, or zookeeper. Default = zookeeper.",
    required = false,
    default = Some("zookeeper"))

  val failoverTimeout = opt[Long]("failover-timeout",
    descr = "Duration in seconds after which to timeout the framework (stopping all tasks); Default = 31536000 (1 year)",
    required = false,
    default = env("FAILOVER_TIMEOUT").map(_.toLong).orElse(Some(31536000L)))

  val apiPort = opt[Int]("api-port",
    descr = s"HTTP API port; can be set via API_PORT; default 8080",
    default = env("API_PORT").map(_.toInt)).orElse(Some(8080))

  val apiHost = opt[String]("api-host",
    descr = s"HTTP API host; can be set via API_HOST; default 127.0.0.1",
    default = env("API_HOST")).orElse(Some("127.0.0.1"))
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
  apiPort: Int = 8080,
  apiHost: String = "127.0.0.1"
) {
  require(AppConfiguration.validStorageBackends.contains(storageBackend))
}

object AppConfiguration {
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
      clusterNetwork = o.clusterNetwork.toOption.getOrElse(o.publicNetwork()),
      storageBackend = o.storageBackend(),
      failoverTimeout = o.failoverTimeout(),
      apiPort = o.apiPort(),
      apiHost = o.apiHost()
    )
  }
}
