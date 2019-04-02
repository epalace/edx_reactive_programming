package kvstore

import akka.actor.Props
import akka.actor.Actor
import akka.actor.ActorRef
import scala.concurrent.duration._

object Replicator {
  case class Replicate(key: String, valueOption: Option[String], id: Long)
  case class Replicated(key: String, id: Long)
  
  case class Snapshot(key: String, valueOption: Option[String], seq: Long)
  case class SnapshotAck(key: String, seq: Long)

  def props(replica: ActorRef): Props = Props(new Replicator(replica))
}

class Replicator(val replica: ActorRef) extends Actor {
  import Replicator._
  import Replica._
  import context.dispatcher
  import scala.concurrent.duration._
  
  /*
   * The contents of this actor is just a suggestion, you can implement it in any way you like.
   */

  // map from sequence number to pair of sender and request
  var acks = Map.empty[Long, (ActorRef, Replicate)]
  // a sequence of not-yet-sent snapshots (you can disregard this if not implementing batching)
 // var pending = Vector.empty[Snapshot]
  
  var _seqCounter = 0L
  def nextSeq() = {
    val ret = _seqCounter
    _seqCounter += 1
    ret
  }

  //private case object SendSnapshots

  private case class RetryReplication(id: Long)

  //context.system.scheduler.schedule(100.milliseconds, 100.milliseconds, self, SendSnapshots)

  def receive: Receive = {
    case msg @ Replicate(key, valueOption, id) =>
      val seq = nextSeq()
      acks = acks.updated(seq, (sender, msg))
      //replica ! Snapshot(key, valueOption, seq)
      self ! RetryReplication(seq)

    case SnapshotAck(key, seq) =>
      if (acks.contains(seq)) {
        val (master, replicate) = acks(seq)
        acks -= seq
        master ! Replicated(key, replicate.id)
      }

    case m @ RetryReplication(seq) =>
      acks.get(seq).foreach {
        case (master, Replicate(key, valueOption, id)) =>
          replica ! Snapshot(key, valueOption, seq)
          context.system.scheduler.scheduleOnce(100.milliseconds, self, m)
      }

//    case SendSnapshots =>
//      acks.toSeq.sortBy(_._1).foreach {
//        case (seq, (master, Replicate(key, valueOption, id))) =>
//          replica ! Snapshot(key, valueOption, seq)
//      }
  }

}
