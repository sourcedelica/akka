package com.nomura.fi.spg.kozo.coreScala.akka

import akka.actor.{ActorLogging, ActorRef, Actor}
import java.util
import akka.serialization.SerializationExtension
import scala.util.{Try, Failure, Success}
import scala.language.existentials
import java.util.UUID

object MessageSequence {
  val maxFrameSizePropName = "akka.remote.netty.tcp.maximum-frame-size"

  private case class Start(id: String, sender: ActorRef, totalSize: Int, chunks: Int, chunkSize: Int,
                           serializerId: Int, clazz: Class[_ <: AnyRef])
  private case class Progress(start: Start, bytes: Array[Byte])
  private case class Chunk(id: String, chunkNumber: Int, bytes: Array[Byte])

  /**
   * This message is sent to the sender after chunking is complete and the
   * original message is added to recipient's mailbox.  If message ordering is important then
   * the next message to the recipient should not be sent until after this is received.
   *
   * @param id The ID of the chunking session.  This will match the value returned by sendChunked().
   */
  case class Complete(id: String)
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

  private val serialization = SerializationExtension(context.system)
  private var inFlight = Map.empty[String, Progress]
  val chunkSize = calcChunkSize

  def receiveChunks: Receive = {
    case s: Start =>
      inFlight += s.id -> Progress(s, new Array[Byte](s.totalSize))

    case chunk @ Chunk(id, chunkNumber, bytes) =>
      inFlight.get(id) match {
        case None => log.error(s"Unexpected chunking $id.  Currently in-flight: ${inFlight.keys}")
        case Some(progress) =>
          val offset = chunkNumber * progress.start.chunkSize
          System.arraycopy(bytes, 0, progress.bytes, offset, bytes.length)

          if (chunkNumber == progress.start.chunks - 1) {
            deserialize(progress.bytes, progress.start.serializerId, progress.start.clazz) match {
              case Failure(ex) => log.error(s"Exception deserializing ${progress.start.id}: ${ex.getMessage}", ex)
              case Success(obj) => 
                self.tell(obj, progress.start.sender)
                progress.start.sender ! Complete(progress.start.id)
            }
            inFlight -= id
          }
      }
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
    val (bytes, serializerId) = serialize(obj)

    val size = bytes.length
    val add = if (size % chunkSize == 0) 0 else 1
    val numChunks = size / chunkSize + add
    val id = generateId
    log.debug(s"Sending chunked ${obj.getClass.getName} to $to, size=$size id=$id")

    to ! Start(id, snd, size, numChunks, chunkSize, serializerId, obj.getClass)

    var chunkNum = 0
    while (chunkNum < numChunks) {
      val start = chunkNum * chunkSize
      val end = Math.min(start + chunkSize, size)
      val chunkBytes = util.Arrays.copyOfRange(bytes, start, end)
      val chunk = Chunk(id, chunkNum, chunkBytes)
      to ! chunk
      chunkNum += 1
    }
    id
  }

  def calcChunkSize: Int = {
    val (baseChunkBytes, _) = serialize(Chunk(null, 0, new Array[Byte](0)))
    val maxFrameSize = context.system.settings.config.getBytes(maxFrameSizePropName).toInt
    val safetyMargin = 2000   // request21.xml required this to be added, investigate
    maxFrameSize - baseChunkBytes.length - safetyMargin
  }
}
