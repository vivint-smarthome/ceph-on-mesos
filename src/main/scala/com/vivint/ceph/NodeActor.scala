// package com.vivint.ceph

// import akka.actor.{ Actor, ActorRef, FSM, Stash }
// import scaldi.Injector
// import scaldi.Injectable._


// object NodeActor {

//   sealed trait Data
//   case object Empty extends Data
//   case class StateData(
//     waitPersistenceVersion: Long,
//     node: TaskActor.NodeState,
//     nextState: Option[State]) extends Data
// }

// class NodeActor(implicit injector: Injector) extends Actor with Stash {
//   import NodeActor._
//   import TaskActor.{TransactionResponse, NodeState, Persist}
//   lazy val taskActor = inject[ActorRef](classOf[TaskActor])
//   // startWith(Initializing, Empty)

//   // when(Initializing) {
//   //   case Event(nodeState: NodeState, _) =>
//   //     if (nodeState.persistentState.isEmpty) {
//   //       taskActor ! TaskActor.Persist(nodeState.taskId)
//   //     // goto(nodestate)
//   //     }
//   //   case Event(_, _) =>
//   //     stash()
//   //     stay
//   // }

//   private var nodeState: NodeState = _

//   def receive = {
//     case n: NodeState =>
//       nodeState = n

//   }

//   def runTransaction(
//     command: TaskActor.Command,
//     onSuccess: () => Unit,
//     onFailure: () => Unit,
//     successP: NodeState => Boolean) = {
//     taskActor ! command
//     var persistedAt: Long = 0
//     val priorTimeout = context.receiveTimeout

//     def checkForTermination() : Unit = {
//       if (nodeState.persistentVersion >= persistedAt)
//         done()
//     }

//     def done(): Unit = {
//       unstashAll()
//       context.unbecome()
//     }


//     context.become({
//       case n: NodeState =>
//         nodeState = n
//         if (successP(nodeState)) {
//         }

//       case TransactionResponse(success, n: NodeState) =>
//         nodeState = n
//         if (success | ) {
//           persistedAt = nodeState.version
//           checkForTermination()
//         } else {
//           done()
//           onFailure()
//         }
//     }, false)

//   }
// }
