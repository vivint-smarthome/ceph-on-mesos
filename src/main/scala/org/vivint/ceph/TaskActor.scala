package org.vivint.ceph

import akka.actor.{ Actor, ActorContext, ActorLogging, ActorRef, Cancellable, FSM, Stash }
import akka.pattern.pipe
import java.util.Collections
import java.util.concurrent.TimeoutException
import org.apache.mesos.Protos._
import org.apache.mesos._
import org.slf4j.LoggerFactory
import org.vivint.ceph.kvstore.{KVStore, CrashingKVStore}
import scaldi.Injectable._
import scaldi.Injector
import scala.concurrent.duration._
import scala.collection.mutable
import scala.collection.breakOut
import scala.collection.JavaConverters._
import org.vivint.ceph.model._

object TaskActor {
  sealed trait State
  case object Initializing extends State
  case object Disconnected extends State
  case object Reconciling extends State
  case object Ready extends State

  sealed trait NodeStatus
  case object Unknown extends NodeStatus
  case class Data(nodes: Map[CephNode, NodeStatus], tasks: Map[String, TaskStatus],
    pendingReconcile: Set[String])
  case class InitialState(nodes: Seq[CephNode], tasks: Seq[TaskStatus])
}


class TaskActor(implicit val injector: Injector) extends FSM[TaskActor.State, TaskActor.Data] with Stash {
  val kvStore = CrashingKVStore(inject[KVStore])
  val taskStore = TaskStore(kvStore)
  val frameworkStore = TaskStore(kvStore)
  val frameworkActor = inject[ActorRef](classOf[FrameworkActor])


  import TaskActor._
  startWith(Initializing, Data(Set.empty, Map.empty, Set.empty))

  override def preStart(): Unit = {
    import context.dispatcher
    taskStore.getNodes.
      zip(taskStore.getTasks).
      map(InitialState.tupled).
      pipeTo(self)
  }

  when(Initializing) {
    case Event(InitialState(nodes, tasks), data) =>
      val nextData = data.copy(
        nodes = nodes.map { n => n -> Unknown }(breakOut),
        tasks = tasks.map { t => t.getTaskId.getValue -> t }(breakOut))
      unstashAll()
      goto(Reconciling) using nextData
    case Event(_, _) =>
      stash()
      stay
  }

  when(Disconnected) {
    case Event(FrameworkActor.Connected, data) =>
      goto(Reconciling) using (data.copy(pendingReconcile = data.tasks.keys.toSet))
  }

  when(Reconciling, 30.seconds) {
    case Event(StateTimeout, _) =>
      throw new TimeoutException("Timed out while reconciling")
    case Event(FrameworkActor.ResourceOffers(offers), _) =>
      offers.foreach { o =>
        frameworkActor ! FrameworkActor.DeclineOffer(o.getId, Some(5.seconds))
      }
      stay
    case Event(FrameworkActor.StatusUpdate(taskStatus), data) =>
      val taskId = taskStatus.getTaskId.getValue
      val nextData = data.copy(
        tasks = data.tasks.updated(taskId, taskStatus),
        pendingReconcile = data.pendingReconcile - taskId)

      if (nextData.pendingReconcile.isEmpty)
        goto(Ready) using (initializeNodeState(nextData))
      else
        stay using (nextData)
  }

  def initializeNodeState(data: Data): Data = {
    // if task running... launch actor
    // if resource reserved... launch actor
    // if task lost but assigned... launch actor
    // if
    ???
  }


  when(Ready) {
    case Event(FrameworkActor.ResourceOffers(offers), data) =>
      // TODO
      // if reservation: known? route to task actor; unknown? destroy / free...
      /* match resource, execute reservation and launch actor. If reserved resource doesn't get re-offered after a
        period of time then crash. Watch for death and put actor back in to pending stage. Task must store
        reservationId, which must be unique per reservation */
      stay
  }

  whenUnhandled {
    case Event(FrameworkActor.Connected, data) =>
      goto(Reconciling) using (data.copy(pendingReconcile = data.tasks.keys.toSet))
  }

  onTransition {
    case _ -> Reconciling =>
      frameworkActor ! FrameworkActor.Reconcile(this.nextStateData.tasks.values.toList)
  }

  // def becomeActive() = {
  //   context.become(active)
  // }

  def reconciling(tasks: Set[TaskStatus]): Receive = {
    case FrameworkActor.StatusUpdate(taskStatus) =>

  }


}
