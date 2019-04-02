package protocols

import akka.actor.typed.Behavior._
import akka.actor.typed.scaladsl.{Behaviors, StashBuffer}
import akka.actor.typed.{ActorContext, Behavior, ExtensibleBehavior, _}

object SelectiveReceive {
    /**
      * @return A behavior that stashes incoming messages unless they are handled
      *         by the underlying `initialBehavior`
      * @param bufferSize      Maximum number of messages to stash before throwing a `StashOverflowException`
      *                        Note that 0 is a valid size and means no buffering at all (ie all messages should
      *                        always be handled by the underlying behavior)
      * @param initialBehavior Behavior to decorate
      * @tparam T Type of messages
      *
      *           Hint: Implement an [[akka.actor.typed.ExtensibleBehavior]], use a [[akka.actor.typed.javadsl.StashBuffer]] and [[akka.actor.typed.Behavior]] helpers such as `start`,
      *           `validateAsInitial`, `interpretMessage`,`canonicalize` and `isUnhandled`.
      */
    def apply[T](bufferSize: Int, initialBehavior: Behavior[T]): Behavior[T] = new ExtensibleBehavior[T] {
        private val buffer = StashBuffer[T](bufferSize)
        validateAsInitial(initialBehavior)

        override def receive(ctx: ActorContext[T], msg: T): Behavior[T] = {
            val next = interpretMessage(initialBehavior, ctx, msg)
            if (isUnhandled(next)) {
                buffer.stash(msg)
                Behaviors.same
            } else {
                buffer.unstashAll(ctx.asScala, apply(bufferSize, canonicalize(next, initialBehavior, ctx)))
            }
        }

        override def receiveSignal(ctx: ActorContext[T], msg: Signal): Behavior[T] = initialBehavior
    }
}
