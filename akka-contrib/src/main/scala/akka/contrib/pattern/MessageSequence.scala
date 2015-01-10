package akka.contrib.pattern

import akka.actor.{Cancellable, ActorLogging, ActorRef, Actor}
import java.util
import akka.serialization.SerializationExtension
import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.util.{Try, Failure, Success}
import scala.language.existentials
import java.util.UUID

object MessageSequence {
  val MaxFrameSizePropName = "akka.remote.netty.tcp.maximum-frame-size"

  private[akka] case class Start(id: String, sender: ActorRef, totalSize: Int, chunks: Int,
                                 serializerId: Int, clazz: Class[_ <: AnyRef])
  private case class Progress(start: Start, bytes: Array[Byte], timeout: Cancellable)
  private case class Chunk(id: String, chunkNumber: Int, bytes: Array[Byte])
  private case class ChunkTimeout(id: String)

  /**
   * This message is sent to the sender after chunking is complete and the
   * original message is added to recipient's mailbox.  If message ordering is important then
   * the next message to the recipient should not be sent until after this is received.
   *
   * @param id The ID of the chunking session.  This will match the value returned by sendChunked().
   */
  case class Complete(id: String)

  /**
   * Sent if a partially completed chunking session times out.
   *
   * @param id The ID of the chunking session.  This will match the value returned by sendChunked().
   */
  case class Timeout(id: String)
}

/**
 * Implementation of the Message Sequence pattern from EIP
 * (http://www.eaipatterns.com/MessageSequence.html) for sending messages larger than
 * akka.remote.netty.tcp.maximum-frame-size.
 *
 * Mix this trait into the actors sending and receiving the large messages.
 *
 * To send, use `sendChunked(to, message)` instead of `to ! message`.
 *
 * To receive, use
 * {{{
 * def receive = receiveChunks orElse {
 *   case ...
 * }
 * }}}
 */

trait MessageSequence { this: Actor with ActorLogging =>
  import MessageSequence._

  private implicit val ec: ExecutionContext = context.system.dispatcher
  private val serialization = SerializationExtension(context.system)
  private var inFlight = Map.empty[String, Progress]
  val chunkSize = calcChunkSize

  /**
   * A partially finished chunking session will timeout after this interval. Override as required.
   */
  val chunkingTimeout: FiniteDuration = 1.hour

  def receiveChunks: Receive = {
    case s: Start =>
      inFlight += s.id -> new Progress(s, new Array[Byte](s.totalSize), scheduleTimeout(s.id))

    case chunk @ Chunk(id, chunkNumber, bytes) =>
      inFlight.get(id) match {
        case None => logMissingInFlight(id)
        case Some(progress) =>
          progress.timeout.cancel()
          log.debug(s"Received #${chunkNumber+1}/${progress.start.chunks} in $id")
          val offset = chunkNumber * chunkSize
          System.arraycopy(bytes, 0, progress.bytes, offset, bytes.length)

          if (chunkNumber == progress.start.chunks - 1) {
            completeChunking(progress)
          } else {
            inFlight += id -> progress.copy(timeout = scheduleTimeout(id))
          }
      }

    case ChunkTimeout(id) =>
      inFlight.get(id) match {
        case None => logMissingInFlight(id)
        case Some(progress) =>
          log.warning(s"Chunk timed out: ${progress.start.id}")
          inFlight -= id
          self ! Timeout(id)
      }
  }

  private def completeChunking(progress: Progress): Unit = {
    deserialize(progress.bytes, progress.start.serializerId, progress.start.clazz) match {
      case Failure(ex) => log.error(s"Exception deserializing ${progress.start.id}: ${ex.getMessage}", ex)
      case Success(obj) =>
        self.tell(obj, progress.start.sender)
        progress.start.sender ! Complete(progress.start.id)
    }
    inFlight -= progress.start.id
  }

  def deserialize(bytes: Array[Byte], serializerId: Int, clazz: Class[_ <: AnyRef]): Try[AnyRef] = {
    serialization.deserialize(bytes, serializerId, Some(clazz))
  }

  def serialize(obj: AnyRef): (Array[Byte], Int) = {
    val serializer = serialization.findSerializerFor(obj)
    (serializer.toBinary(obj), serializer.identifier)
  }

  def generateId: String = UUID.randomUUID.toString

  def sendChunked(to: ActorRef, obj: AnyRef): String = sendChunked(self, to, obj)

  def sendChunked(snd: ActorRef, to: ActorRef, obj: AnyRef): String = {
    val (bytes, start) = startChunked(snd, to, obj)
    sendRange(to, start, bytes, 0, start.chunks)
    start.id
  }

  private[akka] def startChunked(snd: ActorRef, to: ActorRef, obj: AnyRef): (Array[Byte], Start) = {
    val (bytes, serializerId) = serialize(obj)

    val totalSize = bytes.length
    val add = if (totalSize % chunkSize == 0) 0 else 1
    val numChunks = totalSize / chunkSize + add
    val id = generateId
    val start = Start(id, snd, totalSize, numChunks, serializerId, obj.getClass)

    log.debug(s"Sending chunked ${obj.getClass.getName} to $to, size=$totalSize bytes, id=$id")
    to ! start

    (bytes, start)
  }

  private[akka] def sendRange(to: ActorRef, start: Start, bytes: Array[Byte], beginChunk: Int, endChunk: Int) {
    var chunkNum = beginChunk
    while (chunkNum < endChunk) {
      val begin = chunkNum * chunkSize
      val end = Math.min(begin + chunkSize, start.totalSize)
      val chunkBytes = util.Arrays.copyOfRange(bytes, begin, end)
      val chunk = Chunk(start.id, chunkNum, chunkBytes)
      to ! chunk
      chunkNum += 1
    }
  }

  private def calcChunkSize: Int = {
    val (baseChunkBytes, _) = serialize(Chunk(null, 0, new Array[Byte](0)))
    val maxFrameSize = context.system.settings.config.getBytes(MaxFrameSizePropName).toInt
    maxFrameSize - baseChunkBytes.length
  }

  private def logMissingInFlight(id: String) =
    log.error(s"Unexpected chunking $id.  Currently in-flight: ${inFlight.keys}")

  private def scheduleTimeout(id: String): Cancellable =
    context.system.scheduler.scheduleOnce(chunkingTimeout, self, ChunkTimeout(id))
}
