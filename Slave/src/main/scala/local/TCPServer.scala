/**
  * Created by Juwon17 on 2015-11-07.
  */
/**
  * Copyright (C) 2009-2014 Typesafe Inc. <http://www.typesafe.com>
  */

package local

import java.io.{File, FileWriter, BufferedWriter}
import java.net._

import common._

import scala.concurrent.duration.DurationInt

import com.typesafe.config.ConfigFactory

import akka.actor._
import akka.actor.ActorDSL.inbox
import akka.io.{IO, Tcp}
import akka.util.ByteString


class EchoManager(handlerClass: Class[_]) extends Actor with ActorLogging {
  //class EchoManager(listener : ActorRef) extends Actor with ActorLogging {

  import Tcp._
  import context.system

  // there is not recovery for broken connections
  override val supervisorStrategy = SupervisorStrategy.stoppingStrategy

  // bind to the listen port; the port will automatically be closed once this actor dies
  override def preStart(): Unit = {
    IO(Tcp) ! Bind(self, new InetSocketAddress(InetAddress.getLocalHost.getHostAddress, 5151))
  }

  var remoteToFileNumber: Map[InetAddress, Int] = Map()

  // do not restart
  override def postRestart(thr: Throwable): Unit = context stop self

  def receive = {
    case Bound(localAddress) =>
      val port = localAddress.getPort
      println(s"listening on port $port")
    //log.info("listening on port {}", localAddress.getPort)

    case CommandFailed(Bind(_, local, _, _, _)) =>
      log.warning(s"cannot bind to [$local]")
      context stop self

    //#echo-manager
    case Connected(remote, local) =>
      println(s"$local received from $remote")
      //log.info("received connection from {}", remote)
      val fileNumber = remoteToFileNumber.getOrElse(remote.getAddress, 0)
      remoteToFileNumber += (remote.getAddress -> (fileNumber + 1))
      //println(s"remote : $remote, FiNumber : $fileNumber")
      val handler = context.actorOf(Props(handlerClass, sender(), remote))
      sender() ! Register(handler, keepOpenOnPeerClosed = true)
    //#echo-manager
  }

}

/*
object EchoHandler {
  def props(connection: ActorRef, remote: InetSocketAddress): Props =
    Props(classOf[EchoHandler], connection, remote)
}

//#echo-handler
class EchoHandler(connection: ActorRef, remote: InetSocketAddress)
  extends Actor with ActorLogging {

  import Tcp._

  case class Ack(offset: Int) extends Event

  // sign death pact: this actor terminates when connection breaks
  context watch connection

  // start out in optimistic write-through mode
  def receive = writing

  //#writing
  def writing: Receive = {
    case Received(data) =>
      connection ! Write(data, Ack(currentOffset))
      buffer(data)

    case Ack(ack) =>
      acknowledge(ack)

    case CommandFailed(Write(_, Ack(ack))) =>
      connection ! ResumeWriting
      context become buffering(ack)

    case PeerClosed =>
      if (storage.isEmpty) context stop self
      else context become closing
  }
  //#writing

  //#buffering
  def buffering(nack: Int): Receive = {
    var toAck = 10
    var peerClosed = false

    {
      case Received(data)         => buffer(data)
      case WritingResumed         => writeFirst()
      case PeerClosed             => peerClosed = true
      case Ack(ack) if ack < nack => acknowledge(ack)
      case Ack(ack) =>
        acknowledge(ack)
        if (storage.nonEmpty) {
          if (toAck > 0) {
            // stay in ACK-based mode for a while
            writeFirst()
            toAck -= 1
          } else {
            // then return to NACK-based again
            writeAll()
            context become (if (peerClosed) closing else writing)
          }
        } else if (peerClosed) context stop self
        else context become writing
    }
  }
  //#buffering

  //#closing
  def closing: Receive = {
    case CommandFailed(_: Write) =>
      connection ! ResumeWriting
      context.become({

        case WritingResumed =>
          writeAll()
          context.unbecome()

        case ack: Int => acknowledge(ack)

      }, discardOld = false)

    case Ack(ack) =>
      acknowledge(ack)
      if (storage.isEmpty) context stop self
  }
  //#closing

  override def postStop(): Unit = {
    log.info(s"transferred $transferred bytes from/to [$remote]")
  }

  //#storage-omitted
  private var storageOffset = 0
  private var storage = Vector.empty[ByteString]
  private var stored = 0L
  private var transferred = 0L

  val maxStored = 100000000L
  val highWatermark = maxStored * 5 / 10
  val lowWatermark = maxStored * 3 / 10
  private var suspended = false

  private def currentOffset = storageOffset + storage.size

  //#helpers
  private def buffer(data: ByteString): Unit = {
    storage :+= data
    stored += data.size

    if (stored > maxStored) {
      log.warning(s"drop connection to [$remote] (buffer overrun)")
      context stop self

    } else if (stored > highWatermark) {
      log.debug(s"suspending reading at $currentOffset")
      connection ! SuspendReading
      suspended = true
    }
  }

  private def acknowledge(ack: Int): Unit = {
    require(ack == storageOffset, s"received ack $ack at $storageOffset")
    require(storage.nonEmpty, s"storage was empty at ack $ack")

    val size = storage(0).size
    stored -= size
    transferred += size

    storageOffset += 1
    storage = storage drop 1

    if (suspended && stored < lowWatermark) {
      log.debug("resuming reading")
      connection ! ResumeReading
      suspended = false
    }
  }
  //#helpers

  private def writeFirst(): Unit = {
    connection ! Write(storage(0), Ack(storageOffset))
  }

  private def writeAll(): Unit = {
    for ((data, i) <- storage.zipWithIndex) {
      connection ! Write(data, Ack(storageOffset + i))
    }
  }

  //#storage-omitted
}
//#echo-handler


*/

//#simple-echo-handler
class SimpleEchoHandler(connection: ActorRef, remote: InetSocketAddress)
  extends Actor with ActorLogging {

  import Tcp._

  // sign death pact: this actor terminates when connection breaks
  // need to know when the connection dies without sending a `Tcp.ConnectionClosed`
  context watch connection

  case object Ack extends Event

  val dir = new File("fromSlave/%s".format(InetAddress.getLocalHost.getHostAddress))
  dir.mkdirs()
  var fileNumber: Int = 0
  var fw: java.io.FileWriter = _
  var bw: java.io.BufferedWriter = _
  var transferred: Long = _
  var filesize: Long = _

  def receive = {
    case Received(data) if (data.utf8String.split(" ").head == "Start") =>
      //println("========FileStart==========")
      val str = data.utf8String
      println(s"$str")
      fw = new FileWriter("fromSlave/%s/%s".format(InetAddress.getLocalHost.getHostAddress, remote.getAddress + "_" + fileNumber), true)
      bw = new BufferedWriter(fw)
      transferred = 0L
      filesize = data.utf8String.split(" ").tail.tail.head.toInt

      connection ! Write(ByteString("ServerStart"), NoAck)


      context.become({
        /*case Received(data) if (data.utf8String == "End") =>
          connection ! Write(ByteString("ServerEnd"),NoAck)
          println("========FileEnd==========")
          fileNumber += 1
          log.info(s"transferred $transferred bytes") //from/to [$remote]")
          bw.close()
          context.unbecome()
        */
        case Received(data: ByteString) =>
          //println("Received!!!!!!!!!!!!!!")
          transferred += data.utf8String.length
          bw.write(data.utf8String)
          if (transferred == filesize) {
            //println("========FileEnd==========")
            fileNumber += 1
            //log.info(s"transferred $transferred bytes") //from/to [$remote]")
            bw.close()
            context.unbecome()
            connection ! Write(ByteString("ServerEnd"), NoAck)
          }


        case _: Tcp.ConnectionClosed =>
          println("Error : Tcp.ConnectionClosed during transfer")
          bw.close()
          context stop self
      }, discardOld = false)

    case PeerClosed =>
      println("PeerClosed")
      context stop self
    //bw.close()

    case _: Tcp.ConnectionClosed =>
      //println("Tcp.ConnectionClosed")
      log.debug("Stopping, because connection") // for remote address {} closed", remote)
      //bw.close()
      context.stop(self)

    case Terminated(`connection`) =>
      //println("Terminated!!!")
      log.debug("Stopping, because connection") // for remote address {} died", remote)
      //bw.close()
      context.stop(self)
  }

  //#storage-omitted
  //override def postStop(): Unit = {
  //  log.info(s"transferred $transferred bytes from/to [$remote]")
  //}
}

//#simple-echo-handler

