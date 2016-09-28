package org.vivint.ceph

/* Keeps track of task status. Stores state in zookeeper. Handles reconciliation.  ChildTask actors send coordinate with
 * the framework actor to get their resources fulfilled. They have a persistent UUID to uniquely identify them which
 * gets assigned when the task is allocated. This UUID is recorded in a reservation. When a successful reservation is
 * made, the reservation ID is recorded with the task.
 */
class TaskStatusActor {
}
