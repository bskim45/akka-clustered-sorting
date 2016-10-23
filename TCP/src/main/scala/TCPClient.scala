/**
 * Created by Juwon17 on 2015-11-09.
 */

import akka.actor._
import akka.io.Tcp.Write
import akka.io.{ IO, Tcp }
import akka.util.ByteString
import java.net.InetSocketAddress


import scala.io.Source

object TCPClient extends App{
  val system = ActorSystem("TcpClientSystem")
  val testEndPoint = new InetSocketAddress("127.0.0.1", 11111)
  val client = system.actorOf(Client.props(testEndPoint))

}

object Client {
  def props(remote: InetSocketAddress) =
    Props(classOf[Client], remote /*, replies */)
}

class Client(remote: InetSocketAddress /*, listener: ActorRef*/) //extends Actor {

  extends Actor{

    import Tcp._
    import context.system

    IO(Tcp) ! Connect(remote)

    case object Ack extends Event


    def receive = {
      case CommandFailed(_: Connect) =>
        //listener ! "connect failed"
        println("CommandFiled")
        context stop self


      case c@Connected(remote2, local) =>
        //listener ! c
        println("Connected")

        val connection = sender()
        println(s"connection : $connection")
        println(s"remote2 : $remote2")
        println(s"local : $local")
        connection ! Register(self)
        //connection ! Write(ByteString("123"))

        //val ff = Source.fromFile("partition1")
        //println(ff.map(_.toByte).toString())
        //println(ff.mkString)
        //connection ! Write(ByteString(ff.mkString))//(ByteString(ff.map(_.toByte).toString()))
        connection ! Write(ByteString("123"))

        context become {
          case data: ByteString =>
            println("data")
            connection ! Write(data)
          case CommandFailed(w: Write) =>
            // O/S buffer was full
            //listener ! "write failed"
            println("CommandFailed")
          case Received(data) =>
            //listener !
            val str = data.utf8String.trim
            val first_str = str.take(999)
            val end_str = str.takeRight(999)

            println(s"Received front Ack data : $first_str")
            println(s"Received end Ack data : $end_str")
          //connection ! Write(data)
          case "close" =>
            println("close")
            connection ! Close
          case _: ConnectionClosed =>
            //listener ! "connection closed"
            println("connection closed")
            context stop self
          case Tcp.Connected =>
            println("?!!!!?")
        }
    }
  }

