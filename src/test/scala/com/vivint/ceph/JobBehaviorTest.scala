package com.vivint.ceph

import com.vivint.ceph.Directives.{ Directive, OfferResponse, Persist, SetBehaviorTimer }
import com.vivint.ceph.model.{ CephConfigHelper, ClusterSecrets, Job, RunState, TaskState, Location }
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
      response.transition.get shouldBe (jobBehavior.rgw.EphemeralRunning)
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
        case Persist(data) =>
          data.taskId.isEmpty shouldBe false
          data.lastLaunched shouldBe Some(RunState.Running)
          data.slaveId.isEmpty shouldBe false
      }

      inside(offerResponseAction) {
        case OfferResponse(_, List(launchCommand)) =>
          launchCommand.hasLaunch() shouldBe true
      }

      transition shouldBe jobBehavior.rgw.EphemeralRunning
    }

    it("relaunches tasks that are TASK_LOST after timeout") {
      val rgwJob = Job.fromState(
        Workbench.newRunningRGWJob(),
        jobBehavior.defaultBehavior)

      val Directive(Nil, Some(nextBehavior)) = rgwJob.behavior.preStart(rgwJob, Map.empty)

      nextBehavior shouldBe jobBehavior.rgw.EphemeralRunning

      val Directive(List(SetBehaviorTimer(timerId, _)), None) = nextBehavior.preStart(rgwJob, Map.empty)

      val taskLost = rgwJob.copy(taskState = Some(TaskState.TaskLost))

      val Directive(List(SetBehaviorTimer(`timerId`, _)), None) =
        nextBehavior.handleEvent(JobFSM.JobUpdated(rgwJob), taskLost, Map.empty)

      val Directive(List(Persist(relaunchState)), Some(relaunchBehavior)) =
        nextBehavior.handleEvent(JobFSM.Timer(timerId), taskLost, Map.empty)

      relaunchState.taskId shouldBe None
      relaunchState.slaveId shouldBe None
      relaunchState.location shouldBe Location.empty

      relaunchBehavior shouldBe jobBehavior.rgw.MatchAndLaunchEphemeral

    }
  }
}
