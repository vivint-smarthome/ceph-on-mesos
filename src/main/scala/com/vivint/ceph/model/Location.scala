package com.vivint.ceph.model

sealed trait Location {
  def ipOpt: Option[String]
  def portOpt: Option[Int]
  def hostnameOpt: Option[String]
  def withIP(ip: String): Location
}
object Location {
  val empty = PartialLocation(None, None)
}

case class PartialLocation(ip: Option[String], port: Option[Int]) extends Location {
  def ipOpt: Option[String] = ip
  def portOpt: Option[Int] = port
  def hostnameOpt: Option[String] = None
  def withIP(ip: String) = port match {
    case Some(p) => IPLocation(ip, p)
    case None => PartialLocation(Some(ip), None)
  }
}

sealed trait IPLocationLike extends Location {
  def ip: String
  def port: Int

  def ipOpt: Option[String] = Some(ip)
  def portOpt: Option[Int] = Some(port)
}

case class IPLocation(ip: String, port: Int) extends Location with IPLocationLike {
  def withIP(ip: String) = copy(ip = ip)
  def hostnameOpt = None
}

case class ServiceLocation(hostname: String, ip: String, port: Int) extends Location with IPLocationLike {
  def withIP(ip: String) = copy(ip = ip)
  def hostnameOpt = Some(hostname)
}
