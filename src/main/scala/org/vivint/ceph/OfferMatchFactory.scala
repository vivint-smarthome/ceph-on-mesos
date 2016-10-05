package org.vivint.ceph

import java.util.UUID
import mesosphere.marathon.state.{ PersistentVolume, PersistentVolumeInfo }
import mesosphere.mesos.matcher.{ DiskResourceMatcher, ResourceMatcher, ScalarMatchResult, ScalarResourceMatcher }
import mesosphere.mesos.protos.Resource.{CPUS, MEM, DISK, PORTS}
import org.apache.mesos.Protos
import org.vivint.ceph.model.{ CephConfig, NodeState, NodeRole }
import OfferMatchFactory.{OfferMatcher, getPeers}
import scaldi.Injector
import scaldi.Injectable._

object OfferMatchFactory {
  type OfferMatcher = (Protos.Offer, NodeState, Iterable[NodeState]) => Option[ResourceMatcher.ResourceMatch]
  def getPeers(node: NodeState, allNodes: Iterable[NodeState]): Stream[NodeState] = {
    allNodes.toStream.filter { other =>
      other.id != node.id && other.role == node.role
    }
  }
}

trait OfferMatchFactory extends (CephConfig => Map[NodeRole.EnumVal, OfferMatcher]) {
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

  def apply(offer: Protos.Offer, node: NodeState, allNodes: Iterable[NodeState]): Option[ResourceMatcher.ResourceMatch] = {
    val peers = getPeers(node, allNodes)
    val offerSlaveId = offer.getSlaveId.getValue
    val peersAssignedToSlave = peers.map(_.inferPersistedState.slaveId).collect {
      case Some(peerSlaveId) if peerSlaveId == offerSlaveId => 1
    }.length
    if (peersAssignedToSlave < cephConfig.deployment.mon.max_per_host) {
      ResourceMatcher.matchResources(offer, resourceMatchers, selector)
    } else {
      None
    }
  }
}

class MasterOfferMatchFactory(implicit inj: Injector) extends OfferMatchFactory {
  val config = inject[AppConfiguration]

  def apply(cephConfig: CephConfig): Map[NodeRole.EnumVal, OfferMatcher] = {
    Map(
      NodeRole.Monitor -> (new MonOfferMatcher(cephConfig, frameworkRole = config.role))
    )
  }
}
