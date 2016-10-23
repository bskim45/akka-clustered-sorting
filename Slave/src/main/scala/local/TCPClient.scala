/**
  * Created by Juwon17 on 2015-11-09.
  */
package local

import akka.actor._
import akka.io.Tcp.Write
import akka.io.{IO, Tcp}
import akka.util.ByteString
import java.net.{InetAddress, InetSocketAddress}
import Tcp._
import common._
import common.FinishSendingSorted

import scala.collection.mutable.Queue
import scala.collection.mutable.ListBuffer
import scala.io.Source
import java.io.RandomAccessFile


class ClientManager(handlerClass: Class[_], listener: ActorRef) extends Actor {

  case object Ack extends Event

  var testEndPoint: InetSocketAddress = _
  var handler_map: Map[String, ActorRef] = Map((null, listener))
  var handler: ActorRef = _

  def receive = {
    case NotifyPartition(p) =>
      for ((node, (start, end)) <- p) {
        if (node != InetAddress.getLocalHost.getHostAddress) {
          testEndPoint = new InetSocketAddress(node, 5151)
          handler = context.actorOf(Props(handlerClass, testEndPoint, listener))
          //sender() ! Register(handler, keepOpenOnPeerClosed = true) //??
          handler_map = handler_map ++ Map((node, handler))
        }
      }
    case NotifyFile(node, i, filepath) =>
      handler_map.get(node).get ! NotifyFile2(i, filepath)
  }


}

/*
object ClientHandler {
  def props(connection: ActorRef, remote: InetSocketAddress, listener : ActorRef) =
    Props(classOf[ClientHandler], connection, remote, listener )
}
*/

class ClientHandler(remote: InetSocketAddress, listener: ActorRef) extends Actor {

  import context.system

  IO(Tcp) ! Connect(remote)

  case object Ack extends Event

  val buffer_size = 100 * 100

  val byteBuffer = new Array[Byte](buffer_size)

  val failedMessages = new ListBuffer[Write]
  var fileQueue = new Queue[(Long, String)]

  var Size = 0: Long
  var index = 0: Long
  var path: String = _
  var randomFileStream: java.io.RandomAccessFile = _
  var closing = false
  var connection: ActorRef = _
  var ending = false
  var waiting = false

  def receive = {
    case CommandFailed(_: Connect) =>
      //listener ! "connect failed"
      println(" [Client] Connect CommandFailed")
      context stop self

    case c@Connected(remote2, local) =>
      //listener ! c
      //println(s"[Client : $self] ")

      connection = sender()
      println(s" [Client] connection : $connection, remote2 : $remote2, local : $local")
      connection ! Register(self)

      context become {
        case NotifyFile2(fileSize: Long, filePath: String) =>
          if (!waiting) {
            waiting = true
            Size = fileSize
            path = filePath
            //println(" [Client2] Send Start signal")
            val StringSize = Size.toString
            //byteBuffer = new Array[Byte](Size.toInt)
            connection ! Write(ByteString("Start" + " " + s"$path" + " " + s"$StringSize"), NoAck)
          }
          else {
            //println(s" [Client2-2]  $filePath is enqueued")
            fileQueue.enqueue((fileSize, filePath))
          }

        //case Ack =>
        case Received(data) if (data.utf8String == "ServerStart") =>
          waiting = false
          //connection ! WriteFile(filepath, 0, filesize, Ack)
          //val randomAccessFile = new RandomAccessFile(filePath, "r")
          randomFileStream = new RandomAccessFile(path, "r")
          randomFileStream.seek(0)
          randomFileStream.read(byteBuffer)
          index = buffer_size
          val rawString = new String(byteBuffer)
          val rawByte = ByteString(rawString) //rawString.toByte
          //println(s" [Client] total file size to WriteFile: ${fileSize}")
          //context become(buffering, discardOld = false)
          connection ! Write(rawByte, Ack)
          context become(buffering, discardOld = false)

        case CommandFailed(w: Write) =>
          // O/S buffer was full
          //listener ! "write failed"
          println(" [Client] Write CommandFailed")
          failedMessages += w
          connection ! ResumeWriting

        case WritingResumed =>
          failedMessages.foreach(write => connection ! write)
          failedMessages.clear


        case PeerClosed => context stop self
          
        case x =>
          println(s" [Client] Unspecified case : $x")
      }
  }

  def buffering: Receive = {
    case NotifyFile2(fileSize, filePath) =>
      //println(s" [Client3] $filePath is enqueued")
      fileQueue.enqueue((fileSize, filePath))

    case Received(data) if data.utf8String == "ServerEnd" =>

      /*
      println(s" [Client3] index = $index, Size = $Size")
      println(s" [Client3] success to send file {$path}")
      println(" [Client3] Send End signal")
      */
      listener ! FinishSendingSorted(path, remote.getAddress)

      if (fileQueue.isEmpty) {
        if (closing) context stop self
        else context.unbecome()
      }
      else {
        fileQueue.dequeue match {
          case (fSize, fPath) =>
            Size = fSize
            path = fPath
        }
        //println(" [Client3-2] Send Start signal")
        val StringSize = Size.toString
        connection ! Write(ByteString("Start" + " " + s"$path" + " " + s"$StringSize"), NoAck)
        context become( {
          case Received(data) if (data.utf8String == "ServerStart") =>
            randomFileStream = new RandomAccessFile(path, "r")
            randomFileStream.seek(0)
            randomFileStream.read(byteBuffer)
            val rawString = new String(byteBuffer)
            val rawByte = ByteString(rawString)
            connection ! Write(rawByte, Ack)
            index = buffer_size
            context unbecome()

          case NotifyFile2(fileSize, filePath) =>
            //println(s" [Client4-2] $filePath is enqueued")
            fileQueue.enqueue((fileSize, filePath))

          case CommandFailed(w: Write) =>
            // O/S buffer was full
            //listener ! "write failed"
            //println(" [Client4-2] Write CommandFailed")
            failedMessages += w
            connection ! ResumeWriting

          case WritingResumed =>
            failedMessages.foreach(write => connection ! write)
            failedMessages.clear
        }, discardOld = false)
        //connection ! Write(ByteString("Start" + " " + s"$path"), Ack)

      }


    case Ack =>
      if (index < Size) {
        if ((Size - index) < buffer_size) {
          val finishBuffer = new Array[Byte]((Size - index).toInt)
          randomFileStream.read(finishBuffer)
          val rawString = new String(finishBuffer)
          val rawByte = ByteString(rawString)
          connection ! Write(rawByte, Ack)
          index = Size
        }
        else {
          randomFileStream.read(byteBuffer)
          val rawString = new String(byteBuffer)
          val rawByte = ByteString(rawString)
          connection ! Write(rawByte, Ack)
          index += buffer_size
        }
      }


    /*
    else {
      println(s" [Client3] index = $index, Size = $Size")
      println(s" [Client3] success to send file {$path}")
      //if(!ending) {
        println(" [Client3] Send End signal")
        connection ! Write(ByteString("End"), NoAck)
        listener ! FinishSendingSorted(path, remote.getAddress)

        //ending = true
      //}

      /*
      else {
        ending = false

        if (fileQueue.isEmpty) {
          if (closing) context stop self
          else context.unbecome()
        }
        else {
          fileQueue.dequeue match {
            case (fSize, fPath) =>
              Size = fSize
              path = fPath
          }
          println(" [Client3] Send Start signal")
          connection ! Write(ByteString("Start" + " " + s"$path"), NoAck)
          context become ({
            case Received(data) if (data.utf8String == "ServerStart") =>
              randomFileStream = new RandomAccessFile(path, "r")
              randomFileStream.seek(0)
              randomFileStream.read(byteBuffer)
              val rawString = new String(byteBuffer)
              val rawByte = ByteString(rawString)
              connection ! Write(rawByte, Ack)
              index = 100 * 100
              context unbecome()

            case NotifyFile2(fileSize, filePath) =>
              println(s" [Client4] $filePath is enqueued")
              fileQueue.enqueue((fileSize, filePath))

            case CommandFailed(w: Write) =>
              // O/S buffer was full
              //listener ! "write failed"
              println(" [Client4] Write CommandFailed")
              failedMessages += w
              connection ! ResumeWriting

            case WritingResumed =>
              failedMessages.foreach(write => connection ! write)
              failedMessages.clear
          }, discardOld = false)
          //connection ! Write(ByteString("Start" + " " + s"$path"), Ack)

        }
      }*/
    }
  */
    case CommandFailed(w: Write) =>
      // O/S buffer was full
      //listener ! "write failed"
      //println(" [Client3] Write CommandFailed")
      failedMessages += w
      connection ! ResumeWriting

    case WritingResumed =>
      failedMessages.foreach(write => connection ! write)
      failedMessages.clear

    case PeerClosed => closing = true

    case _: ConnectionClosed =>
      println(" [Client] ConnectionClosed!")
      context stop self

    case x =>
      println(s"[Client] Unspecified case : $x")
  }

}


/*
object Client {
  def props(remote: InetSocketAddress , filesize:Long, filepath : String, listener : ActorRef) =
    Props(classOf[Client], remote, filesize:Long, filepath : String, listener : ActorRef)
}

class Client(remote: InetSocketAddress, filesize:Long, filepath : String, listener: ActorRef) //extends Actor {

  extends Actor{

    import Tcp._
    import context.system

    IO(Tcp) ! Connect(remote)

    case object Ack extends Event

    val byteBuffer = new Array[Byte](100 * 100)

    val failedMessages = new ListBuffer[Write]

    var Size = 0:Long
    var index = 0:Long

    def receive = {
      case CommandFailed(_: Connect) =>
        //listener ! "connect failed"
        println(" [Client] Connect CommandFailed")
        context stop self

      case c@Connected(remote2, local) =>
        //listener ! c
        println(s"[Client : $self] ")

        val connection = sender()
        println(s" [Client] connection : $connection, remote2 : $remote2, local : $local")
        connection ! Register(self)

        Size = filesize

        //connection ! WriteFile(filepath, 0, filesize, Ack)

        val randomAccessFile = new RandomAccessFile(filepath, "r")

        randomAccessFile.seek(0)
        randomAccessFile.read(byteBuffer)
        index += 100*100
        val rawString = new String(byteBuffer)
        val rawByte = ByteString(rawString) //rawString.toByte

        println(s" [Client] total file size to WriteFile: ${filesize}")

        connection ! Write(rawByte, Ack)

        context become {
          case data: ByteString =>
            println(s"data : $data")
          case CommandFailed(w: Write) =>
            // O/S buffer was full
            //listener ! "write failed"
            println(" [Client] Write CommandFailed")
            failedMessages += w
            connection ! ResumeWriting
          case Received(data) =>
            val str = data.utf8String
            println(s"[Client] Received{$str}")
          case Ack =>
            if(Size < filesize) {
              randomAccessFile.read(byteBuffer)
              val rawString = new String(byteBuffer)
              val rawByte = ByteString(rawString)
              connection ! Write(rawByte, Ack)
              index += 100*100
            }
            else {
              println(s" [Client] success to send file {$filepath}")
              listener ! FinishSendingSorted(filepath, remote2.getAddress)
              connection ! Close
            }
          case WritingResumed =>
            failedMessages.foreach(write => connection ! write)
            failedMessages.clear
          case _: ConnectionClosed =>
            println(" [Client] ConnectionClosed!")
            context stop self
          case x =>
            println(" [Client] Unspecified case : x")
        }
    }
  }
*/

