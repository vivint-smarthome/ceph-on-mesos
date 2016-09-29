package org.vivint.ceph.model

import com.typesafe.config.{ Config, ConfigFactory }
import configs.{ ConfigError, FromString, Result }
import mesosphere.marathon.state.DiskType
import configs.syntax._


case class MonDeploymentConfig(
  count: Int,
  cpus: Double,
  mem: Double,
  diskType: DiskType,
  disk: Long
)

case class OSDDeploymentConfig(
  count: Int,
  cpus: Double,
  mem: Double,
  diskType: DiskType,
  disk: Long,
  maxSize: Option[Long],
  pathConstraint: Option[String]
)


object ConfigHelpers {
}

case class DeploymentConfig(
  mon: MonDeploymentConfig,
  osd: OSDDeploymentConfig
)

object DeploymentConfigHelper {
  val defaultConfig = ConfigFactory.parseString("""
mon {
  diskType = root
  disk = 16
}

osd {
  diskType = root
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

  def parse(str: String): DeploymentConfig = {
    val config = defaultConfig.withFallback(ConfigFactory.parseString(str))
    config.extract[DeploymentConfig].value
  }
}

// object MonDeploymentConfig {
//   import ConfigHelpers._
//   def parse(config: Config): MonDeploymentConfig = {
//     config.get[MonDeploymentConfig]("mon").value
//   }

//   // MonDeploymentConfig(
//   //   count = config.get[Int]("count").value,
//   //   cpus = config.get[Double]("cpus").value,
//   //   mem = config.getDouble("mem"),
//   //   ram = config.getDouble("ram"),
//   //   disk = config.getDouble("disk")
//   // )
// }

// // object OSDDeploymentConfig {
// //   def parse(config: Config): OSDDeploymentConfig = OSDDeploymentConfig(
// //     count = config.getInt("count"),
// //     cpus = config.getDouble("cpus"),
// //     mem = config.getDouble("mem"),
// //     ram = config.getDouble("ram"),
// //     diskType = config.get("diskType"),
// //     disk = config.getDouble("disk"),
// //     maxSize = config.getDouble("maxSize"),
// //     pathConstraint = Try(config.getString("pathConstraint")).toOption
// //   )

// // }


// // object DeploymentConfig {
// // }
