package kvstore

import akka.actor.{ OneForOneStrategy, Props, ActorRef, Actor }
import kvstore.Arbiter._
import scala.collection.immutable.Queue
import akka.actor.SupervisorStrategy.Restart
import scala.annotation.tailrec
import akka.pattern.{ ask, pipe }
import akka.actor.Terminated
import scala.concurrent.duration._
import akka.actor.PoisonPill
import akka.actor.OneForOneStrategy
import akka.actor.SupervisorStrategy
import akka.util.Timeout

object Replica {
  sealed trait Operation {
    def key: String
    def id: Long
  }
  case class Insert(key: String, value: String, id: Long) extends Operation
  case class Remove(key: String, id: Long) extends Operation
  case class Get(key: String, id: Long) extends Operation

  sealed trait OperationReply
  case class OperationAck(id: Long) extends OperationReply
  case class OperationFailed(id: Long) extends OperationReply
  case class GetResult(key: String, valueOption: Option[String], id: Long) extends OperationReply

  def props(arbiter: ActorRef, persistenceProps: Props): Props = Props(new Replica(arbiter, persistenceProps))
}

class Replica(val arbiter: ActorRef, persistenceProps: Props) extends Actor {
  import Replica._
  import Replicator._
  import Persistence._
  import context.dispatcher

  private var kv = Map.empty[String, String]
  private var secondaries = Map.empty[ActorRef, ActorRef]

  arbiter ! Join

  private var expectedSnapshotSeq = 0L

  private case class Status(client: ActorRef, key: String, valueOption: Option[String], persisted: Boolean = false, replicatorsAck: Set[ActorRef] = Set.empty){
    def replicatedInAll(replicators: Set[ActorRef]) = replicators.forall(replicatorsAck)
  }

  private val persistence = context.actorOf(persistenceProps)
  private var pending = Map.empty[Long, Status]

  private case class RetryPersist(id: Long)
  private case class CheckFailure(id: Long)
  //private case object SendFailures


//  private def setSchedulers(): Unit = {
//   // context.system.scheduler.schedule(100.milliseconds, 100.milliseconds, self, RetryPersist)
//    //context.system.scheduler.schedule(1.seconds, 1.seconds, self, SendFailures)
//    ()
//  }

  //override def supervisorStrategy = ???

  override def receive = {
    case JoinedPrimary   => context.become(leader)
    case JoinedSecondary => context.become(replica)
  }

//  private def retryPersist(): Unit =
//    pending.toSeq.sortBy(_._1).foreach {
//      case (id, status) =>
//        if (!status.persisted) {
//          persistence ! Persist(status.key, status.valueOption, id)
//        }
//    }

  private def doOperation(key: String, valueOpt: Option[String], id: Long): Unit = {
    pending += (id -> Status(sender, key, valueOpt))
    persistence ! Persist(key, valueOpt, id)
    secondaries.values.foreach {
      replicator => replicator ! Replicate(key, valueOpt, id)
    }
    context.system.scheduler.scheduleOnce(1.seconds, self, CheckFailure(id))
    context.system.scheduler.scheduleOnce(100.milliseconds, self, RetryPersist(id))
    ()
  }

  private def retryPersist(id: Long): Unit = {
    pending.get(id).foreach { status =>
      if (!status.persisted) {
        persistence ! Persist(status.key, status.valueOption, id)
        context.system.scheduler.scheduleOnce(100.milliseconds, self, RetryPersist(id))
      }
    }
  }


  /* TODO Behavior for  the leader role. */
  private val leader: Receive = {
    case Insert(key, value, id) =>
      kv += (key -> value)
      doOperation(key, Some(value), id)

    case Remove(key, id) =>
      kv -= key
      doOperation(key, None, id)

    case Get(key, id) =>
      sender ! GetResult(key, kv.get(key), id)

    case Persisted(key, id) =>
      pending.get(id).foreach { status =>
        if (status.replicatedInAll(secondaries.values.toSet)) {
          status.client ! OperationAck(id)
          pending -= id
        } else {
          pending += (id -> status.copy(persisted = true))
        }
      }

    case Replicated(key, id) =>
      pending.get(id).foreach { status =>
        val newStatus = status.copy(replicatorsAck = status.replicatorsAck + sender)
        pending += (id -> newStatus)
        if (status.persisted && newStatus.replicatedInAll(secondaries.values.toSet)) {
          pending -= id
          status.client ! OperationAck(id)
        }
      }

    case RetryPersist(id: Long) => retryPersist(id)


    case CheckFailure(id: Long) =>
      pending.get(id).foreach { status =>
        status.client ! OperationFailed(id)
        pending -= id
      }

//    case SendFailures =>
//      pending.foreach {
//        case (id, status) => status.client ! OperationFailed(id)
//      }
//      pending = Map.empty[Long, Status]

    case Replicas(replicas) =>
      val newSecondaries = (replicas - self)

      val added = (newSecondaries -- secondaries.keys).map(replica => replica -> context.actorOf(Props(new Replicator(replica)))).toMap

      //start replicating to added
      added.values.foreach { replicator =>
        var id = -1
        //TODO should we send the whole map and then the pending operations?
        kv.foreach { case (key, value) =>
            replicator ! Replicate(key, Some(value), id)
            id -= 1
        }

        pending.toSeq.sortBy(_._1).foreach { case (id, status) =>
          replicator ! Replicate(status.key, status.valueOption, id)
        }
      }

      val removed = secondaries -- newSecondaries
      removed.values.foreach { context.stop } //stop replicators of replicas that have left

      secondaries = (secondaries -- removed.keys) ++ added

      if (removed.nonEmpty) {
        //since some replicas have left, some pending acks might fulfill now their replication factor
        pending.foreach {
          case (id, status) =>
            if (status.replicatedInAll(secondaries.values.toSet) && status.persisted) {
              pending -= id
              status.client ! OperationAck(id)
            }
        }
      }


  }

  private val replica: Receive = {
    case Get(key, id) =>
      sender ! GetResult(key, kv.get(key), id)

    case Snapshot(key, valueOpt, seq)  =>
      if (seq < expectedSnapshotSeq) {
        sender ! SnapshotAck(key, seq) //ignore snapshot but ack
      } else if (seq == expectedSnapshotSeq) {
        //apply snapshot and persist
        valueOpt match {
          case Some(value) => kv += (key -> value)
          case None => kv -= key
        }
        pending += (seq -> Status(sender, key, valueOpt))
        persistence ! Persist(key, valueOpt, seq)
        context.system.scheduler.scheduleOnce(100.milliseconds, self, RetryPersist(seq))
        ()
      } else {
        //do nothing
      }

    case Persisted(key, id) =>
      if (id == expectedSnapshotSeq) {
        val status = pending(id)
        pending -= id
        expectedSnapshotSeq += 1
        status.client ! SnapshotAck(key, id)
      }

    case RetryPersist(id: Long) =>
      retryPersist(id)

  }

}

