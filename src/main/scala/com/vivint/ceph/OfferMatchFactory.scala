package com.vivint.ceph

import java.util.UUID
import mesosphere.marathon.state.{ PersistentVolume, PersistentVolumeInfo, DiskType }
import mesosphere.mesos.matcher.{ DiskResourceMatcher, ResourceMatcher, ScalarMatchResult, ScalarResourceMatcher }
import mesosphere.mesos.protos.Resource.{CPUS, MEM, DISK, PORTS}
import org.apache.mesos.Protos
import com.vivint.ceph.model.{ CephConfig, Task, TaskRole }
import OfferMatchFactory.{OfferMatcher, getPeers, peersAssignedToSlave}
import scaldi.Injector
import scaldi.Injectable._

object OfferMatchFactory {
  type OfferMatcher = (Protos.Offer, Task, Iterable[Task]) => Option[ResourceMatcher.ResourceMatch]
  def getPeers(task: Task, allTasks: Iterable[Task]): Stream[Task] = {
    allTasks.toStream.filter { other =>
      other.id != task.id && other.role == task.role
    }
  }

  def peersAssignedToSlave(slaveId: Protos.SlaveID, task: Task, allTasks: Iterable[Task]): Int = {
    val peers = getPeers(task, allTasks)
    val offerSlaveId = slaveId.getValue
    peers.map(_.pState.slaveId).collect {
      case Some(peerSlaveId) if peerSlaveId == offerSlaveId => 1
    }.length
  }
}

trait OfferMatchFactory extends (CephConfig => Map[TaskRole.EnumVal, OfferMatcher]) {
}

class RGWOfferMatcher(cephConfig: CephConfig, frameworkRole: String) extends OfferMatcher {
  val selector = ResourceMatcher.ResourceSelector.any(Set("*", frameworkRole))

  val resourceMatchers = {
    val selector = ResourceMatcher.ResourceSelector.any(Set("*", frameworkRole))
    val rgwConfig = cephConfig.deployment.rgw

    val portMatcher = if (rgwConfig.port.isEmpty)
      Some(new lib.SinglePortMatcher(selector))
    else
      None

    List(
      new ScalarResourceMatcher(
        CPUS, cephConfig.deployment.rgw.cpus, selector, ScalarMatchResult.Scope.NoneDisk),
      new ScalarResourceMatcher(
        MEM, cephConfig.deployment.rgw.mem, selector, ScalarMatchResult.Scope.NoneDisk)
    ) ++ portMatcher
  }

  def apply(offer: Protos.Offer, task: Task, allTasks: Iterable[Task]): Option[ResourceMatcher.ResourceMatch] = {
    val count = peersAssignedToSlave(offer.getSlaveId, task, allTasks)
    if (count < cephConfig.deployment.rgw.max_per_host) {
      ResourceMatcher.matchResources(offer, resourceMatchers, selector)
    } else {
      None
    }
  }
}

class OSDOfferMatcher(cephConfig: CephConfig, frameworkRole: String) extends OfferMatcher {
  val selector = ResourceMatcher.ResourceSelector.any(Set("*", frameworkRole))

  val resourceMatchers = {
    val selector = ResourceMatcher.ResourceSelector.any(Set("*", frameworkRole))
    val osdConfig = cephConfig.deployment.osd

    val volume = PersistentVolume.apply(
      "state",
      PersistentVolumeInfo(
        osdConfig.disk,
        `maxSize` = osdConfig.disk_max.filter(_ => osdConfig.disk_type == DiskType.Mount),
        `type` = cephConfig.deployment.osd.disk_type),
      Protos.Volume.Mode.RW)

    List(
      new ScalarResourceMatcher(
        CPUS, cephConfig.deployment.osd.cpus, selector, ScalarMatchResult.Scope.NoneDisk),
      new ScalarResourceMatcher(
        MEM, cephConfig.deployment.osd.mem, selector, ScalarMatchResult.Scope.NoneDisk),
      new DiskResourceMatcher(
        selector, 0.0, List(volume), ScalarMatchResult.Scope.IncludingLocalVolumes),
      new lib.ContiguousPortMatcher(5,
        selector))
  }

  def apply(offer: Protos.Offer, task: Task, allTasks: Iterable[Task]): Option[ResourceMatcher.ResourceMatch] = {
    val count = peersAssignedToSlave(offer.getSlaveId, task, allTasks)
    if (count < cephConfig.deployment.osd.max_per_host) {
      ResourceMatcher.matchResources(offer, resourceMatchers, selector)
    } else {
      None
    }
  }
}

class MonOfferMatcher(cephConfig: CephConfig, frameworkRole: String) extends OfferMatcher {
  val selector = ResourceMatcher.ResourceSelector.any(Set("*", frameworkRole))

  val resourceMatchers = {
    val selector = ResourceMatcher.ResourceSelector.any(Set("*", frameworkRole))

    val volume = PersistentVolume.apply(
      "state",
      PersistentVolumeInfo(
        cephConfig.deployment.mon.disk,
        `type` = cephConfig.deployment.mon.disk_type),
      Protos.Volume.Mode.RW)

    // TODO - if matching reserved resources set matchers appropriately
    List(
      new ScalarResourceMatcher(
        CPUS, cephConfig.deployment.mon.cpus, selector, ScalarMatchResult.Scope.NoneDisk),
      new ScalarResourceMatcher(
        MEM, cephConfig.deployment.mon.mem, selector, ScalarMatchResult.Scope.NoneDisk),
      new DiskResourceMatcher(
        selector, 0.0, List(volume), ScalarMatchResult.Scope.IncludingLocalVolumes),
      new lib.SinglePortMatcher(
        selector))
  }

  def apply(offer: Protos.Offer, task: Task, allTasks: Iterable[Task]): Option[ResourceMatcher.ResourceMatch] = {
    val count = peersAssignedToSlave(offer.getSlaveId, task, allTasks)
    if (count < cephConfig.deployment.mon.max_per_host) {
      ResourceMatcher.matchResources(offer, resourceMatchers, selector)
    } else {
      None
    }
  }
}

class MasterOfferMatchFactory(implicit inj: Injector) extends OfferMatchFactory {
  val config = inject[AppConfiguration]

  def apply(cephConfig: CephConfig): Map[TaskRole.EnumVal, OfferMatcher] = {
    Map(
      TaskRole.Monitor -> (new MonOfferMatcher(cephConfig, frameworkRole = config.role)),
      TaskRole.OSD -> (new OSDOfferMatcher(cephConfig, frameworkRole = config.role)),
      TaskRole.RGW -> (new RGWOfferMatcher(cephConfig, frameworkRole = config.role))
    )
  }
}
