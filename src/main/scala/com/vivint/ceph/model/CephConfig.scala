package com.vivint.ceph.model

import com.typesafe.config.{ Config, ConfigFactory, ConfigObject }
import configs.FromString
import mesosphere.marathon.state.DiskType
import configs.syntax._
import java.nio.charset.StandardCharsets.UTF_8


case class MonDeploymentConfig(
  count: Int,
  cpus: Double,
  mem: Double,
  disk_type: DiskType,
  disk: Long,
  max_per_host: Int
)

case class OSDDeploymentConfig(
  count: Int,
  cpus: Double,
  mem: Double,
  disk_type: DiskType,
  disk: Long,
  disk_max: Option[Long],
  path_constraint: Option[String],
  max_per_host: Int
)

case class RGWDeploymentConfig(
  count: Int,
  cpus: Double,
  mem: Double,
  port: Option[Int],
  docker_args: Map[String, String],
  max_per_host: Int
)

case class DeploymentConfig(
  mon: MonDeploymentConfig,
  osd: OSDDeploymentConfig,
  rgw: RGWDeploymentConfig
)

case class CephSettings(
  global: ConfigObject,
  auth: ConfigObject,
  mon: ConfigObject,
  osd: ConfigObject,
  client: ConfigObject,
  mds: ConfigObject
)

case class CephConfig(
  deployment: DeploymentConfig,
  settings: CephSettings
)


object CephConfigHelper {
  val defaultConfig = ConfigFactory.parseString("""
deployment {
  mon {
    disk_type = root
    disk = 16
    max_per_host = 1
  }

  osd {
    disk_type = root
    max_per_host = 1
  }

  rgw {
    max_per_host = 1
    docker_args {
    }
  }
}

settings {
  auth {}
  global {}
  mon {}
  osd {}
  client {}
  mds {}
}
""")

  implicit val readDiskType: FromString[DiskType] =
    FromString.fromTry { str =>
      DiskType.all.
        find(_.toString == str).
        getOrElse {
          throw(new RuntimeException(s"${str} is not a valid disk type"))
        }
    }

  def parse(str: String): CephConfig = {
    val config = ConfigFactory.parseString(str).withFallback(defaultConfig)
    config.extract[CephConfig].value
  }

  def parse(bytes: Array[Byte]): CephConfig = {
    parse(new String(bytes, UTF_8))
  }

}
