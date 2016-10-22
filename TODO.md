# Functionality

## definitely

- [ ] Permit OSD btrfs deployment
- [ ] Permissive resource releases. It would be better that a bug result in resources not being released, than release
      resources that shouldn't be released.
- [ ] Health checks. Leader launch pattern should wait for leader node to return successful health before launching
      other tasks.
- [ ] add orchestrator events such as rolling restart, rolling repair, etc.
- [ ] Version running states so we can rolling restart the nodes.
- [ ] Support Mesos SSL
- [ ] Support SSL for framework endpoint.
- [ ] Add authentication for SSL framework endpoint. (SSL client auth?)
- [ ] Support for CEPH deploying metadata servers
- [ ] Support for launching Ceph on other container engines, such as RKT.

## maybe

- [ ] Consider supporting unsupported file systems by allocating contiguous blocks.
- [ ] Configurable secret to protect access to pull ceph config

## done

- [x] Simple web UI
- [x] Configurable ceph-docker image, allowing local / private repository to be used.
- [x] Packaging
- [x] Mesos DNS discovery / srv records [0.2.0]
- [x] Exclusive lock. It would be castostrophic if two ceph-on-mesos frameworks launched concurrently. [0.1.0]
- [x] Extract leader launch pattern into orchestrator [0.1.0]
- [x] Simple API endpoints to pull configuration necessary to connect to ceph. [0.1.0]
- [x] Unique reservation IDs to survive SlaveID changes.

# Code cleanup

- [x] Extract task state tracking from TaskActor. Guard operations.
- [x] Emit TaskChanged messages as part of nodeupdated; concern too scattered.
- [x] extract TaskFSM concerns from TaskActor.
- [ ] Extract launch strategy from node behavior and extract common behaviors into it's own library.
- [ ] Consider emitting separate events for different kinds of node changes. IE - goal updated, task status changed,
      lastLaunched changed.

