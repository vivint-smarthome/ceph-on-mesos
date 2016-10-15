package com.vivint.ceph

import java.util.UUID
import mesosphere.marathon.Protos.Constraint
import mesosphere.marathon.Protos.Constraint.Operator
import mesosphere.marathon.state.{ PersistentVolume, PersistentVolumeInfo, DiskType }
import mesosphere.mesos.matcher.{ DiskResourceMatcher, ResourceMatcher, ScalarMatchResult, ScalarResourceMatcher }
import mesosphere.mesos.protos.Resource.{CPUS, MEM, DISK, PORTS}
import org.apache.mesos.Protos
import com.vivint.ceph.model.{ CephConfig, Job, JobRole }
import OfferMatchFactory.{OfferMatcher, getPeers, peersAssignedToSlave, newPathConstraints}
import scaldi.Injector
import scaldi.Injectable._

object OfferMatchFactory {
  type OfferMatcher = (Protos.Offer, Job, Iterable[Job]) => Option[ResourceMatcher.ResourceMatch]
  def getPeers(task: Job, allTasks: Iterable[Job]): Stream[Job] = {
    allTasks.toStream.filter { other =>
      other.id != task.id && other.role == task.role
    }
  }

  def peersAssignedToSlave(slaveId: Protos.SlaveID, task: Job, allTasks: Iterable[Job]): Int = {
    val peers = getPeers(task, allTasks)
    val offerSlaveId = slaveId.getValue
    peers.map(_.pState.slaveId).collect {
      case Some(peerSlaveId) if peerSlaveId == offerSlaveId => 1
    }.length
  }

  def newPathConstraints(matcherOpt: Option[String]): Set[Constraint] = matcherOpt match {
    case Some(matcher) =>
      Set(
        Constraint.newBuilder.
          setField("path").
          setOperator(Operator.LIKE).
          setValue(matcher).
          build)
    case None =>
      Set.empty
  }
}

trait OfferMatchFactory extends (CephConfig => Map[JobRole.EnumVal, OfferMatcher]) {
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

  def apply(offer: Protos.Offer, task: Job, allTasks: Iterable[Job]): Option[ResourceMatcher.ResourceMatch] = {
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
        constraints = newPathConstraints(cephConfig.deployment.osd.path_constraint),
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

  def apply(offer: Protos.Offer, task: Job, allTasks: Iterable[Job]): Option[ResourceMatcher.ResourceMatch] = {
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
        constraints = newPathConstraints(cephConfig.deployment.mon.path_constraint),
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

  def apply(offer: Protos.Offer, task: Job, allTasks: Iterable[Job]): Option[ResourceMatcher.ResourceMatch] = {
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

  def apply(cephConfig: CephConfig): Map[JobRole.EnumVal, OfferMatcher] = {
    Map(
      JobRole.Monitor -> (new MonOfferMatcher(cephConfig, frameworkRole = config.role)),
      JobRole.OSD -> (new OSDOfferMatcher(cephConfig, frameworkRole = config.role)),
      JobRole.RGW -> (new RGWOfferMatcher(cephConfig, frameworkRole = config.role))
    )
  }
}
