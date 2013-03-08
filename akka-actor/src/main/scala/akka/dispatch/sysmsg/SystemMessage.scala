/**
 * Copyright (C) 2009-2013 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.dispatch.sysmsg

import scala.annotation.tailrec
import akka.actor.{ ActorRef, PossiblyHarmful }

object SystemMessageList {
  final val LNil: LatestFirstSystemMessageList = new LatestFirstSystemMessageList(null)
  final val ENil: EarliestFirstSystemMessageList = new EarliestFirstSystemMessageList(null)

  @tailrec
  private[sysmsg] def sizeInner(head: SystemMessage, acc: Int): Int = if (head eq null) acc else sizeInner(head.next, acc + 1)

  @tailrec
  private[sysmsg] def reverseInner(head: SystemMessage, acc: SystemMessage): SystemMessage = {
    if (head eq null) acc else {
      val next = head.next
      head.next = acc
      reverseInner(next, head)
    }
  }
}

class LatestFirstSystemMessageList(val head: SystemMessage) extends AnyVal {
  import SystemMessageList._

  final def isEmpty: Boolean = head eq null
  final def size: Int = sizeInner(head, 0)
  final def tail: LatestFirstSystemMessageList = new LatestFirstSystemMessageList(head.next)
  final def reverse: EarliestFirstSystemMessageList = new EarliestFirstSystemMessageList(reverseInner(head, null))

  final def ::(msg: SystemMessage): LatestFirstSystemMessageList = {
    assert(msg ne null)
    msg.next = head
    new LatestFirstSystemMessageList(msg)
  }

}

class EarliestFirstSystemMessageList(val head: SystemMessage) extends AnyVal {
  import SystemMessageList._

  final def isEmpty: Boolean = head eq null
  final def size: Int = sizeInner(head, 0)
  final def tail: EarliestFirstSystemMessageList = new EarliestFirstSystemMessageList(head.next)
  final def reverse: LatestFirstSystemMessageList = new LatestFirstSystemMessageList(reverseInner(head, null))

  final def ::(msg: SystemMessage): EarliestFirstSystemMessageList = {
    assert(msg ne null)
    msg.next = head
    new EarliestFirstSystemMessageList(msg)
  }

  final def reversePrepend(other: LatestFirstSystemMessageList): EarliestFirstSystemMessageList = {
    var remaining = other
    var result = this
    while (!remaining.isEmpty) {
      val msg = remaining.head
      remaining = remaining.tail
      result ::= msg
    }
    result
  }

}

/**
 * System messages are handled specially: they form their own queue within
 * each actor’s mailbox. This queue is encoded in the messages themselves to
 * avoid extra allocations and overhead. The next pointer is a normal var, and
 * it does not need to be volatile because in the enqueuing method its update
 * is immediately succeeded by a volatile write and all reads happen after the
 * volatile read in the dequeuing thread. Afterwards, the obtained list of
 * system messages is handled in a single thread only and not ever passed around,
 * hence no further synchronization is needed.
 *
 * INTERNAL API
 *
 * ➡➡➡ NEVER SEND THE SAME SYSTEM MESSAGE OBJECT TO TWO ACTORS ⬅⬅⬅
 */
private[akka] sealed trait SystemMessage extends PossiblyHarmful with Serializable {
  // Next fields are only modifiable via the SystemMessageList value class
  @transient
  private[sysmsg] var next: SystemMessage = _

  def unlink(): Unit = next = null

  def unlinked: Boolean = next eq null
}

/**
 * INTERNAL API
 */
@SerialVersionUID(-4836972106317757555L)
private[akka] case class Create(uid: Int) extends SystemMessage // send to self from Dispatcher.register
/**
 * INTERNAL API
 */
@SerialVersionUID(686735569005808256L)
private[akka] case class Recreate(cause: Throwable) extends SystemMessage // sent to self from ActorCell.restart
/**
 * INTERNAL API
 */
@SerialVersionUID(7270271967867221401L)
private[akka] case class Suspend() extends SystemMessage // sent to self from ActorCell.suspend
/**
 * INTERNAL API
 */
@SerialVersionUID(-2567504317093262591L)
private[akka] case class Resume(causedByFailure: Throwable) extends SystemMessage // sent to self from ActorCell.resume
/**
 * INTERNAL API
 */
@SerialVersionUID(708873453777219599L)
private[akka] case class Terminate() extends SystemMessage // sent to self from ActorCell.stop
/**
 * INTERNAL API
 */
@SerialVersionUID(3245747602115485675L)
private[akka] case class Supervise(child: ActorRef, async: Boolean, uid: Int) extends SystemMessage // sent to supervisor ActorRef from ActorCell.start
/**
 * INTERNAL API
 */
@SerialVersionUID(5513569382760799668L)
private[akka] case class ChildTerminated(child: ActorRef) extends SystemMessage // sent to supervisor from ActorCell.doTerminate
/**
 * INTERNAL API
 */
@SerialVersionUID(3323205435124174788L)
private[akka] case class Watch(watchee: ActorRef, watcher: ActorRef) extends SystemMessage // sent to establish a DeathWatch
/**
 * INTERNAL API
 */
@SerialVersionUID(6363620903363658256L)
private[akka] case class Unwatch(watchee: ActorRef, watcher: ActorRef) extends SystemMessage // sent to tear down a DeathWatch
/**
 * INTERNAL API
 */
@SerialVersionUID(-5475916034683997987L)
private[akka] case object NoMessage extends SystemMessage // switched into the mailbox to signal termination

/**
 * INTERNAL API
 */
@SerialVersionUID(3L)
private[akka] case class Failed(child: ActorRef, cause: Throwable, uid: Int) extends SystemMessage