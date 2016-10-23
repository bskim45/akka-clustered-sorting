package local

import java.io._
import java.net.{InetAddress, InetSocketAddress}

import akka.actor.{Actor, ActorRef, Props}
import common.Constants._
import common.SharedTypes._
import common.{NotifyFile, Utils}

import scala.collection.mutable.ArrayBuffer

case class SortChunk(partition: Partition, chunkFile: File)

case object SortChunkDone


object ChunkSortActor {
  def props(listener: ActorRef) =
    Props(classOf[ChunkSortActor], listener: ActorRef)
}


class ChunkSortActor(client: ActorRef) extends Actor {

  var mPartition: Partition = List()
  var testEndPoint: InetSocketAddress = _

  val byteBuffer = new Array[Byte](LINE_SIZE)

  def receive = {
    case SortChunk(partition, chunkFile) =>
      mPartition = partition
      sortChunk(chunkFile)
      sender ! SortChunkDone
  }

  def readChunk(f: File): ArrayBuffer[Entry] = {
    val chunk = ArrayBuffer.empty[Entry]

    val fr = new FileReader(f)
    val br = new BufferedReader(fr)

    var row = br.readLine()
    while (row != null) {
      chunk.append(row.splitAt(10))
      row = br.readLine()
    }

    br.close()
    fr.close()

    chunk
  }

  def sortChunk(f: File): Unit = {

    //println("ChunkSortActor sorting: " + f.getName)

    // read entries
    val chunk = readChunk(f).sortBy(_._1)

    // sort
    //    Sorting.quickSort(chunk.toA)(Ordering.by[(String, String), String](_._1))

    // for debug use: print all keys as 'int array'
    //    for((k, v) <- chunk) {
    //      println(k.toString.map(c => c.toInt))
    //    }

    for ((node, (start, end)) <- mPartition) {
      var i = 0

//      println("Sorting for node: " + node)
      val partedChunk = chunk.filter({ case (k, _) => start <= k && k < end })

      // write sorted entries to file
      val dir = new File("temp/" + node)
      dir.mkdirs()

      val fw = new FileWriter("temp/%s/%s".format(node, f.getName), false)
      val bw = new BufferedWriter(fw)

      for ((k, v) <- partedChunk) {
        bw.write("%s%s\r\n".format(k, v))
        i = i + 1
      }

      bw.close()
      fw.close()

      i *= 100

      if (node != InetAddress.getLocalHost.getHostAddress)
        client ! NotifyFile(node, i, "temp/%s/%s".format(node, f.getName))
      else {
        // create merge dir
        val dir = new File("fromSlave/" + Utils.getIP)
        if (!dir.exists())
          dir.mkdirs()

        val file = new File("temp/%s/%s".format(node, f.getName))
        file.renameTo(new File("fromSlave/%s/%s".format(Utils.getIP, f.getName)))
      }
    }

    chunk.clear()

  }
}

