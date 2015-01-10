package akka.contrib.pattern

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FunSuite, BeforeAndAfterAll}
import akka.testkit.TestKit
import akka.actor._
import java.util
import com.nomura.fi.spg.kozo.coreScala.util.TestNameFunSuite
import concurrent.duration._
import akka.serialization.SerializationExtension

// TODO: change to use AkkaSpec

object MessageSequenceSpec {
  val ChunkTimeout = 5.seconds

  object Message {
    val testInt = 1
    val testDouble = 2.0
    val testString = "foo"

    def apply(sender: ActorRef, length: Int): Message =
      Message(sender, new Array[Byte](length), testInt, testDouble, testString)
  }

  case class Message(sender: ActorRef, bytes: Array[Byte], i: Int, d: Double, s: String) {
    util.Arrays.fill(bytes, 0.toByte)
  }

  class ChunkingSender(receiver: ActorRef) extends Actor with ActorLogging with MessageSequence {
    def receive = {
      case MessageSequence.Complete(_) =>
      case obj: AnyRef => sendChunked(receiver, obj)
    }
  }

  class ChunkingReceiver(tester: ActorRef) extends Actor with ActorLogging with MessageSequence {
    def receive = receiveChunks orElse {
      case msg: Message if msg.sender == sender => tester ! msg
      case x => println(s"ERROR:unexpected $x, sender: ${sender()}")  // expect will time out
    }
  }

  case class ChunkSize(size: Int)

  class ChunkingSizer(tester: ActorRef) extends Actor with ActorLogging with MessageSequence {
    tester ! ChunkSize(chunkSize)
    self ! PoisonPill

    def receive = { case _ => }
  }

  class TimeoutSender(tester: ActorRef, receiver: ActorRef, maxChunks: Int) extends Actor with ActorLogging with MessageSequence {
    case class Next(count: Int)

    def receive = {
      case c @ MessageSequence.Complete(_) => tester ! c
      case obj: AnyRef =>
        val (b, s) = startChunked(sender(), receiver, obj)
        self ! Next(0)
        context become started(b, s)
    }

    def started(bytes: Array[Byte], start: MessageSequence.Start): Receive = {
      case Next(count) =>
        if (count < maxChunks && count < start.chunks) sendRange(receiver, start, bytes, count, count + 1)
        self ! Next(count + 1)
    }
  }

  class TimeoutReceiver(tester: ActorRef) extends Actor with ActorLogging with MessageSequence {
    override val chunkingTimeout = ChunkTimeout

    def receive = receiveChunks orElse {
      case msg: Message => tester ! msg  // If this happens them expectMsgClass will fail
      case t @ MessageSequence.Timeout(id) => tester ! t
      case x => println(s"ERROR: unexpected $x, sender: ${sender()}")   // expect will time out
    }
  }
}

@RunWith(classOf[JUnitRunner])
class MessageSequenceSpec extends TestKit(ActorSystem("ChunkingTest")) with FunSuite with BeforeAndAfterAll {
  import MessageSequenceSpec._

  val maxFrameSize = system.settings.config.getBytes(MessageSequence.MaxFrameSizePropName).toInt

  def assertMessageSize(testName: String, length: Int) {
    // Forwards Messages back to us
    val receiver = system.actorOf(Props(new ChunkingReceiver(testActor)), testName + "Receiver")

    // Implements Chunking, forwards chunked Messages to ChunkingReceiver
    val sender = system.actorOf(Props(new ChunkingSender(receiver)), testName + "Sender")

    val msg = Message(sender, length)
    sender ! msg

    import Message._

    expectMsgPF(10.seconds, s"Message with array($length) i=$testInt d=$testDouble s=$testString") {
      case msg: Message
        if msg.bytes.length == length && msg.i == testInt && msg.d == testDouble && msg.s == testString => msg
    }

    system.stop(receiver)
    system.stop(sender)
  }

  test("larger than frame size") {
    assertMessageSize("largerThanFrameSize", maxFrameSize * 10 + maxFrameSize / 2)
  }

  test("less than frame size") {
    assertMessageSize("lessThanFrameSize", 0)
  }
  
  test("multiple of chunk size") {
    val testName = "multipleOfChunkSize"
    system.actorOf(Props(new ChunkingSizer(testActor)), testName + "Sizer")
    val chunkSize = expectMsgClass(classOf[ChunkSize])

    val serialization = SerializationExtension(system)
    val msgSize = serialization.serialize(Message(null, 0)).getOrElse(fail("could not serialize message")).length

    assertMessageSize(testName, chunkSize.size - msgSize)
  }

  test("timeout immediately after start") {
    assertTimeout("timeoutImmediatelyAfterStart", 0)
  }

  test("timeout") {
    assertTimeout("timeout", 2)
  }

  def assertTimeout(testName: String, maxChunks: Int) {
    // Forwards Messages back to us
    val receiver = system.actorOf(Props(new TimeoutReceiver(testActor)), testName + "Receiver")

    // Implements Chunking, would forward chunked Messages to TimeoutReceiver, but drops chunks after 1st
    val sender = system.actorOf(Props(new TimeoutSender(testActor, receiver, maxChunks)), testName + "Sender")

    val msg = Message(sender, maxFrameSize * 2)
    sender ! msg

    expectMsgClass(ChunkTimeout * 2, classOf[MessageSequence.Timeout])

    system.stop(receiver)
    system.stop(sender)
  }

  override protected def afterAll() {
    system.shutdown()
  }  
}
