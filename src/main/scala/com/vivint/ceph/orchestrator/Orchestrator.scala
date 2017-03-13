package com.vivint.ceph.orchestrator

import com.vivint.ceph.JobsState

object Orchestrator {
  /**
    * Bootstraps the orchestrator
    */
  def run(jobs: JobsState): Unit = {
    val orchestratorFSM = new OrchestratorFSM(Bootstrap.Start, jobs)
    jobs.addSubscriber(orchestratorFSM.update)
  }
}
