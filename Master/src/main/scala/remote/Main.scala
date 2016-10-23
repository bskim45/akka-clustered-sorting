package remote

import java.net.InetAddress

import akka.actor.{ActorSystem, Props}
import common.Start

object Main {
  val usage =
    """
    Usage: master <# of slaves>
    """

  def main(args: Array[String]): Unit = {
    if (args.length == 0) {
      println(usage)
      System.exit(1)
    }

    val slaveNum = args.head
    val system = ActorSystem("HelloRemoteSystem")
    val remoteActor = system.actorOf(Props(new RemoteActor(slaveNum.toInt)), name = "RemoteActor")

    // print master address
    println("master " + InetAddress.getLocalHost.getHostAddress + ":" + 5150)

    remoteActor ! Start
  }

}