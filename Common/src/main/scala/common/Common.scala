package common

import java.io.File
import java.net.InetAddress

import common.SharedTypes._

case object Start
case class Message(msg: String)
case object Establish
case object EstablishAck
case object RequestSample
case class SubmitSample(sample: Sample)
case object SubmitSampleAck
case class NotifyPartition(partition : Partition)
case class FinishSort(filepath: String)
case class FinishSendingSorted(filepath:String, remote:InetAddress)
case class FinishSending(fn : Int)
case object SortComplete
case class NotifyFile(node : String, fileSize : Long, filepath : String)
case class NotifyFile2(fileSize : Long, filepath : String)
case class MergeChunk(phase: Int, index: Int, indexList: List[Int])
case class MergeChunkDone(phase: Int, index: Int)
case object StartMerge
case object FinishMerge
case object SlaveTerminate
case object KeepAlive

object Constants {
  val CHUNK_SIZE = 32000000       // 32MB
  val LINE_SIZE = 100             // 100B
  val MAXIMUM_PAYLOAD = 128000    // 128KB
  val KEY_SIZE = 10               // 10B
}

object SharedTypes {
  type Sample = List[String]
  type OptionMap = Map[Symbol, Any]
  type Partition = List[(String, (String, String))]
  type Entry = (String, String)

  lazy val samplePartition: Partition = {
    def buildKey(n: Int): String = {
      var k = ""
      for (i <- 1 to 10)
        k += n.toChar

      k
    }

    List(("192.168.0.1", (buildKey(0), buildKey(64))), ("192.168.0.2", (buildKey(64), buildKey(127))))
  }
}


object Utils {
  val IPRegx = "\\b(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\\b".r
  val IPPortRegx = "\\b(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?):\\d{1,5}\\b".r

  def parseIP(s: String): String = {
    IPRegx.findFirstIn(s).get.toString
  }

  def getIP: String = {
    InetAddress.getLocalHost.getHostAddress
  }

  def intToString(n: Int): String = {
    val c = n.toChar
    def iter(i: Int): String = {
      if (i > 1) c +: iter(i - 1)
      else c.toString
    }
    iter(10)
  }

  def getListOfFiles(dir: String, recursive: Boolean): List[File] = {
    def recursiveListFiles(d: File): List[File] = {
      if (d.exists && d.isDirectory) {
        if (recursive) d.listFiles.filter(_.isFile).toList ++ d.listFiles.filter(_.isDirectory).flatMap(recursiveListFiles)
        else d.listFiles.filter(_.isFile).toList
      } else {
        List[File]()
      }
    }

    recursiveListFiles(new File(dir))
  }

  def deleteDirectory(dir: String): Boolean = {
    def deleteDirHelper(path: File): Boolean = {

      if (!path.exists) return false

      path.listFiles.foreach(f => {
        if (f.isDirectory) {
          deleteDirHelper(f)
        } else {
          f.delete()
        }
      })

      path.delete()
    }

    deleteDirHelper(new File(dir))
  }
}

