package com.vivint.ceph

import java.util.UUID
import org.apache.mesos.Protos
import com.vivint.ceph.model.{ Location, TaskState }
import com.vivint.ceph.model.{ JobRole, Job }
import JobFSM._
import Behavior._
import scala.concurrent.duration._
import scaldi.Injector
import scaldi.Injectable._
import ProtoHelpers._
import Directives._
import Behaviors.LaunchBehaviorFactory

object Behaviors {
  /**
    LaunchBehaviorFactory function behavioral contracts:

    - should return a task with a unique taskId
    - should return as much information as derivable from the offer where the task is to be run
  */
  type LaunchBehaviorFactory = (Job, Map[UUID, Job], Protos.Offer) => (Location, Protos.TaskInfo)
}

case class Behaviors(
  // TODO - we should just pass this in rather than have a getter function; don't initialize this code until framework
  // connected
  frameworkId: Protos.FrameworkID,
  launchBehavior: LaunchBehaviorFactory)(implicit inj: Injector) {

  val offerOperations = inject[OfferOperations]

  def decideWhatsNext(state: Job, fullState: Map[UUID, Job]): Directives.Directive = {
    import Directives._
    state.pState match {
      case pState if state.version != state.persistentVersion =>
        // try again; previous persistence must have timed out.
        Persist(pState).
          withTransition(
            WaitForSync(decideWhatsNext))
      case pState =>
        if (pState.reservationConfirmed)
          Transition(Running)
        else if (pState.slaveId.nonEmpty)
          Transition(WaitForReservation)
        else
          Transition(MatchForReservation)
    }
  }

  case object InitializeResident extends Behavior {
    override def preStart(state: Job, fullState: Map[UUID, Job]): Directive = {
      decideWhatsNext(state: Job, fullState: Map[UUID, Job]): Directive
    }

    def handleEvent(event: Event, state: Job, fullState: Map[UUID, Job]): Directive =
      throw new IllegalStateException("handleEvent called on InitializeResident")
  }

  case class WaitForSync(nextBehavior: DecideFunction) extends Behavior {
    override def preStart(state: Job, fullState: Map[UUID, Job]): Directive = {
      SetBehaviorTimer("timeout", 30.seconds)
    }

    def handleEvent(event: Event, state: Job, fullState: Map[UUID, Job]): Directive = {
      event match {
        case Timer("timeout") =>
          nextBehavior(state, fullState)
        case Timer(_) =>
          Stay
        case JobUpdated(prior) =>
          if (state.persistentVersion < state.version)
            Stay
          else
            nextBehavior(state, fullState)
        case MatchedOffer(offer, matchResult) =>
          Hold(offer, matchResult)
      }
    }
  }

  case object MatchForReservation extends Behavior {
    override def preStart(state: Job, fullState: Map[UUID, Job]): Directive = {
      WantOffers
    }

    def handleEvent(event: Event, state: Job, fullState: Map[UUID, Job]): Directive = {
      event match {
        case Timer(_) =>
          Stay
        case JobUpdated(_) =>
          Stay
        case MatchedOffer(pendingOffer, matchResult) =>
          val resources = pendingOffer.offer.resources
          if (resources.forall { r => r.hasReservation }) {
            val newState = state.pState.copy(
              reservationConfirmed = true,
              slaveId              = Some(pendingOffer.slaveId))
            Persist(newState) andAlso Hold(pendingOffer, matchResult) withTransition (Running)
          } else {
            val reservationId = UUID.randomUUID()
            matchResult match {
              case Some(result) =>
                OfferResponse(
                  pendingOffer,
                  offerOperations.reserveAndCreateVolumes(
                    frameworkId,
                    jobId         = state.id,
                    reservationId = reservationId,
                    resourceMatch = result)).
                  andAlso(
                    Persist(
                      state.pState.copy(
                        slaveId       = Some(pendingOffer.slaveId),
                        reservationId = Some(reservationId)))).
                  withTransition(WaitForReservation)
              case None =>
                OfferResponse(pendingOffer, Nil) andAlso WantOffers
            }
          }
      }
    }
  }

  case object WaitForReservation extends Behavior {
    override def preStart(state: Job, fullState: Map[UUID, Job]): Directive = {
      SetBehaviorTimer("timeout", 30.seconds)
    }

    def handleEvent(event: Event, state: Job, fullState: Map[UUID, Job]): Directive = {
      event match {
        case Timer("timeout") =>
          Transition(MatchForReservation)
        case Timer(_) =>
          Stay
        case MatchedOffer(pendingOffer, matchResult) =>
          if (pendingOffer.offer.resources.exists(_.hasReservation)) {
            val newState = state.pState.copy(
              reservationConfirmed = true)
            Persist(newState).
              andAlso(Hold(pendingOffer, matchResult)).
              withTransition(WaitForSync(decideWhatsNext))
          } else {
            Hold(pendingOffer, matchResult).withTransition(MatchForReservation)
          }
        case JobUpdated(_) =>
          Stay
      }
    }
  }

  case class Sleep(duration: FiniteDuration, andThen: DecideFunction) extends Behavior {
    override def preStart(state: Job, fullState: Map[UUID, Job]): Directive = {
      SetBehaviorTimer("wakeup", duration)
    }

    def handleEvent(event: Event, state: Job, fullState: Map[UUID, Job]): Directive = {
      event match {
        case Timer("wakeup") =>
          andThen(state, fullState)
        case Timer(_) | JobUpdated(_) =>
          Stay
        case MatchedOffer(offer, matchResult) =>
          Hold(offer, matchResult)
      }
    }
  }


  val holdingOffers: PartialFunction[JobFSM.Event, Directive] = {
    case MatchedOffer(offer, matchResult) =>
      Hold(offer, matchResult)
  }

  case class Killing(duration: FiniteDuration, andThen: TransitionFunction) extends Behavior {
    override def preStart(state: Job, fullState: Map[UUID, Job]): Directive = {
      if (state.runningState.isEmpty)
        throw new IllegalStateException("can't kill a non-running task")

      SetBehaviorTimer("timeout", duration).andAlso(KillTask)
    }

    def handleEvent(event: Event, state: Job, fullState: Map[UUID, Job]): Directive = {
      handleWith(event) {
        holdingOffers orElse {
          case Timer("timeout") =>
            preStart(state, fullState)
          case JobUpdated(_) if (state.runningState.isEmpty) =>
            Transition(andThen(state, fullState))
        }
      }
    }
  }

  case class WaitForGoal(andThen: TransitionFunction) extends Behavior {
    def handleEvent(event: Event, state: Job, fullState: Map[UUID, Job]): Directive = {
      handleWith(event) {
        holdingOffers orElse {
          case JobUpdated(_) if (state.goal.nonEmpty) =>
            Transition(andThen(state, fullState))
        }
      }
    }
  }

  case object MatchAndLaunchEphemeral extends Behavior {
    override def preStart(state: Job, fullState: Map[UUID, Job]): Directive = {
      if (state.goal.isEmpty)
        Transition(WaitForGoal{ (_,_) => MatchAndLaunchEphemeral})
      else if (state.taskId.nonEmpty)
        // If the task is dead, lost, or fails to return a status, EphemeralRunning will relaunch
        Transition(EphemeralRunning)
      else
        WantOffers
    }

    def handleEvent(event: Event, state: Job, fullState: Map[UUID, Job]): Directive = event match {
      case MatchedOffer(pendingOffer, matchResult) =>
        val (location, taskInfo) = launchBehavior(state, fullState, pendingOffer.offer)

        Persist(
          state.pState.copy(
            slaveId = Some(pendingOffer.offer.getSlaveId.getValue),
            lastLaunched = state.goal,
            taskId = Some(taskInfo.getTaskId.getValue),
            location = location
          )).
          andAlso(
            OfferResponse(
              pendingOffer,
              List(
                newOfferOperation(
                  newLaunchOperation(
                    Seq(taskInfo)))))).
          withTransition(EphemeralRunning)
      case _ =>
        Stay
    }
  }

  case object EphemeralRunning extends Behavior {
    override def preStart(state: Job, fullState: Map[UUID, Job]): Directive = {
      if (state.taskId.isEmpty)
        throw new IllegalStateException(s"Can't be EphemeralRunning without a taskId")
      if (state.slaveId.isEmpty)
        throw new IllegalStateException(s"Can't be EphemeralRunning without a slaveId")

      decideWhatsNext(state)
    }

    def relaunch(state:Job): Directive =
      Persist(state.pState.copy(taskId = None, slaveId = None, location = Location.empty)).
        withTransition(MatchAndLaunchEphemeral)

    def decideWhatsNext(state: Job): Directive = {
      state.taskState match {
        case Some(_: TaskState.Terminal) =>
          relaunch(state)
        case None | Some(_: TaskState.Limbo) =>
          SetBehaviorTimer("timeout", 60.seconds)
        case other =>
          if (state.goal != state.lastLaunched)
            Transition(Killing(70.seconds, { (_, _) => MatchAndLaunchEphemeral }))
          else
            Stay
      }
    }

    def handleEvent(event: Event, state: Job, fullState: Map[UUID, Job]): Directive = event match {
      case JobUpdated(_) =>
        decideWhatsNext(state)
      case Timer("timeout") =>
        if (state.taskState.isEmpty || state.taskState.exists(_.isInstanceOf[TaskState.Limbo])) {
          // task failed to launch. Try again.
          relaunch(state)
        } else {
          Stay
        }
      case Timer(_) =>
        Stay
      case MatchedOffer(offer, _) =>
        OfferResponse(offer, Nil)
    }
  }

  case object Running extends Behavior {
    def reservationConfirmed(state:Job) =
      state.pState.reservationConfirmed

    def getMonitors(fullState: Map[UUID, Job]) =
      fullState.values.filter(_.role == JobRole.Monitor).toList

    override def preStart(state: Job, fullState: Map[UUID, Job]): Directive = {
      if(! state.reservationConfirmed)
        throw new IllegalStateException("Can't go to running state without a confirmed reservation")
      Revive andAlso nextRunAction(state, fullState)
    }

    def nextRunAction(state: Job, fullState: Map[UUID, Job]): Directive = {
      (state.runningState, state.goal) match {
        case (_, None) =>
          Transition(WaitForGoal( { (_,_) => Running }))

        case (Some(running), Some(goal)) if running != goal =>
          // current running state does not match goal
          Transition(Killing(70.seconds, { (_, _) => Running }))

        case (_, Some(_)) =>
          // if we already have a goal then proceed
          Stay
      }
    }

    def handleEvent(event: Event, state: Job, fullState: Map[UUID, Job]): Directive = {
      event match {
        case Timer(_) =>
          Stay
        case MatchedOffer(pendingOffer, _) =>
          if (state.runningState.nonEmpty)
            /* We shouldn't get the persistent offer for a task unless if the task is dead, so we shouldn't have to
             * worry about this. Unless if two persistent offers were made for a task, on the same slave... which semes
             * very unlikely */
            throw new IllegalStateException("As assumption made by the framework author was wrong")

          if(!pendingOffer.offer.resources.exists(_.hasReservation))
            OfferResponse(pendingOffer, Nil)
          else {
            val (location, taskInfo) = launchBehavior(state, fullState, pendingOffer.offer)

            Persist(
              state.pState.copy(
                location = location,
                taskId = Some(taskInfo.getTaskId.getValue),
                slaveId = Some(pendingOffer.offer.slaveId.get),
                lastLaunched = state.goal)).
              andAlso(
                OfferResponse(
                  pendingOffer,
                  List(
                    newOfferOperation(
                      newLaunchOperation(
                        Seq(taskInfo))))))
          }
        case JobUpdated(prior) =>
          // if the goal has changed then we need to revaluate our next run state
          val next = nextRunAction(state, fullState)
          if (prior.runningState.nonEmpty && state.runningState.isEmpty) /* oops we lost it! */ {
            Revive andAlso next
          } else {
            next
          }
      }
    }
  }
}
