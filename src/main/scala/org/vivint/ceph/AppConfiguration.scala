package org.vivint.ceph

import org.rogach.scallop._

class CephFrameworkOptions(args: List[String]) extends ScallopConf(args) {
  val master = opt[String]("master", 'm',
    required = true,
    descr = "Mesos master location; in zk://ip1:port1,ip2:port2/mesos format")

  val name = opt[String]("name", 'n', descr = "framework name", default = Some("ceph"))

  val principal = opt[String]("principal", 'p',
    descr = "mesos principal as which to authenticate; can be set via MESOS_PRINCIPAL env var",
    default = Option(System.getenv("MESOS_PRINCIPAL")))

  val secret = opt[String]("secret", 's',
    descr = "mesos principal as which to authenticate; can be set via MESOS_SECRET env var",
    default = Option(System.getenv("MESOS_SECRET")))

  val zookeeper = opt[String]("zookeeper", 'z',
    required = true,
    descr = "Location for zookeeper for ceph-framework to store it's state")

  verify()
}
case class AppConfiguration(
  master: String,
  name: String,
  principal: Option[String],
  secret: Option[String],
  zookeeper: String)

object AppConfiguration extends ((String, String, Option[String], Option[String], String) => AppConfiguration) {
  def fromArgs(args: List[String]): AppConfiguration = {
    val o = new CephFrameworkOptions(args)
    AppConfiguration(
      master = o.master(),
      name = o.name(),
      principal = o.principal.toOption,
      secret = o.secret.toOption,
      zookeeper = o.zookeeper())
  }
}
