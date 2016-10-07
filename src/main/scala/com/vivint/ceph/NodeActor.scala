// package com.vivint.ceph

// import akka.actor.{ Actor, ActorRef, FSM, Stash }
// import scaldi.Injector
// import scaldi.Injectable._


// object TaskActor {

//   sealed trait Data
//   case object Empty extends Data
//   case class StateData(
//     waitPersistenceVersion: Long,
//     task: TaskActor.TaskState,
//     nextState: Option[State]) extends Data
// }

// class TaskActor(implicit injector: Injector) extends Actor with Stash {
//   import TaskActor._
//   import TaskActor.{TransactionResponse, TaskState, Persist}
//   lazy val taskActor = inject[ActorRef](classOf[TaskActor])
//   // startWith(Initializing, Empty)

//   // when(Initializing) {
//   //   case Event(taskState: TaskState, _) =>
//   //     if (taskState.persistentState.isEmpty) {
//   //       taskActor ! TaskActor.Persist(taskState.taskId)
//   //     // goto(taskstate)
//   //     }
//   //   case Event(_, _) =>
//   //     stash()
//   //     stay
//   // }

//   private var taskState: TaskState = _

//   def receive = {
//     case n: TaskState =>
//       taskState = n

//   }

//   def runTransaction(
//     command: TaskActor.Command,
//     onSuccess: () => Unit,
//     onFailure: () => Unit,
//     successP: TaskState => Boolean) = {
//     taskActor ! command
//     var persistedAt: Long = 0
//     val priorTimeout = context.receiveTimeout

//     def checkForTermination() : Unit = {
//       if (taskState.persistentVersion >= persistedAt)
//         done()
//     }

//     def done(): Unit = {
//       unstashAll()
//       context.unbecome()
//     }


//     context.become({
//       case n: TaskState =>
//         taskState = n
//         if (successP(taskState)) {
//         }

//       case TransactionResponse(success, n: TaskState) =>
//         taskState = n
//         if (success | ) {
//           persistedAt = taskState.version
//           checkForTermination()
//         } else {
//           done()
//           onFailure()
//         }
//     }, false)

//   }
// }
