package com.vivint.ceph

import com.vivint.ceph.Directives.{ Directive, OfferResponse, SetBehaviorTimer }
import com.vivint.ceph.model.{ CephConfigHelper, ClusterSecrets, Job, RunState }
import org.scalatest.{ FunSpec, Inside, Matchers }
import scaldi.Module

class JobBehaviorTest extends FunSpec with Matchers with Inside {
  val module = new Module {
    bind [OfferOperations] to new OfferOperations
    bind [views.ConfigTemplates] to new views.ConfigTemplates
    bind [AppConfiguration] to Workbench.newAppConfiguration()
  }

  import module.injector
  val jobBehavior = new JobBehavior(
    ClusterSecrets.generate,
    log = null,
    frameworkId = { () => MesosTestHelper.frameworkID },
    deploymentConfig = { () => CephConfigHelper.parse(ConfigStore.default) }
  )
  describe("MatchAndLaunchEphemeral") {
    it("waits for the task status to be known before requesting offers") {
      val rgwJob = Job.fromState(Workbench.newRunningRGWJob(), jobBehavior.defaultBehavior)

      val response = rgwJob.behavior.preStart(rgwJob, Map.empty)
      response.action shouldBe Nil
      response.transition.get shouldBe (jobBehavior.EphemeralRunning)
    }

    it("persists lastLaunched when accepting an offer") {
      val rgwJob = Job.fromState(
        Workbench.newPendingRGWJob(
          goal = Some(RunState.Running)),
        jobBehavior.defaultBehavior)

      val Directive(List(action), None) = rgwJob.behavior.preStart(rgwJob, Map.empty)
      action shouldBe (Directives.WantOffers)

      val pendingOffer = PendingOffer(MesosTestHelper.makeBasicOffer().build)

      val Directive(List(persistAction, offerResponseAction), Some(transition)) =
        rgwJob.behavior.handleEvent(JobFSM.MatchedOffer(pendingOffer, None), rgwJob, Map.empty)

      inside(persistAction) {
        case Directives.Persist(data) =>
          data.taskId.isEmpty shouldBe false
          data.lastLaunched shouldBe Some(RunState.Running)
          data.slaveId.isEmpty shouldBe false
      }

      inside(offerResponseAction) {
        case OfferResponse(_, List(launchCommand)) =>
          launchCommand.hasLaunch() shouldBe true
      }

      transition shouldBe jobBehavior.EphemeralRunning
    }
  }
}
