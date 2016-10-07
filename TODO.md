# Functionality

## definitely

- Simple web UI
- Permissive resource releases. It would be better that a bug result in resources not being released, than release
  resources that shouldn't be released.
- Health checks. Leader launch pattern should wait for leader node to return successful health before launching other
  tasks.
- Extract leader launch pattern into orchestrator
- Version running states so we can rolling restart the nodes.

## maybe

- Consider supporting non-XFS file systems by allocating contiguous blocks.

# Code cleanup

- Extract task state tracking from TaskActor. Guard operations.
- Emit TaskChanged messages as part of nodeupdated; concern too scattered.
- Extract launch strategy from node behavior.
- Extract common behaviors into it's own library.
- Consider emitting separate events for different kinds of node changes. IE - goal updated, task status changed,
  lastLaunched changed.

