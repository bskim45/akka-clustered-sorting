package local

import java.io._

import akka.actor.{Actor, Props}
import common.Constants._
import common.SharedTypes.Entry
import common.Utils._
import common.{MergeChunk, MergeChunkDone}

import scala.annotation.tailrec
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class SortedChunk(p:Int, i:Int){
  val BUFFER_SIZE = 100000000
  val FETCH_LINES = BUFFER_SIZE / LINE_SIZE
  var fileIndex = 0
  val parts = getListOfFiles("merge/" + getIP, false).filter(_.getName.startsWith("merge_%d_%d_".format(p - 1, i))).toVector.sortBy(_.getName.split("_", 4)(3))
  var reader : FileReader = new FileReader(parts(0))
  var buffer : BufferedReader = new BufferedReader(reader)
  val lines = mutable.Queue[Entry]()//ArrayBuffer[Entry]()
  var empty : Boolean = false
  var firstRow : Option[Entry] = _

  /*
  def close(): Unit = {
    buffers.foreach(_.close)
    readers.foreach(_.close)
  }

  */

  def delete(): Unit = {
    parts.foreach(_.delete())
  }

  def readRows() : Unit = {
    if (fileIndex == parts.length) return

    @tailrec
    def readRowsHelper(n : Int) : Unit = {
      val row = buffer.readLine()
      if(row == null){
        fileIndex = fileIndex + 1
        buffer.close()
        reader.close()

        if (fileIndex == parts.length) return

        reader = new FileReader(parts(fileIndex))
        buffer = new BufferedReader(reader)
        lines.enqueue(buffer.readLine.splitAt(10))
        //lines.append(buffer.readLine.splitAt(10))
        readRowsHelper(79)
      }
      else if (n == 1) lines.enqueue(row.splitAt(10)) //lines.append(row.splitAt(10))
      else {
        //lines.append(row.splitAt(10))
        lines.enqueue(row.splitAt(10))
        readRowsHelper(n-1)
      }
    }

    //BufferedReader buffer size is 8kb so 80 line read
    readRowsHelper(80)

    /*
    val row = buffer.readLine

    if(row == null){
      buffer.close()
      reader.close()

      //parts delete

      fileIndex = fileIndex + 1
      if(fileIndex == parts.length) return

      reader = new FileReader(parts(fileIndex))
      buffer = new BufferedReader(reader)

      lines.append(buffer.readLine().splitAt(10))
    }
    else lines.append(row.splitAt(10))
    */
  }

  def getFirstRow: Option[Entry] = {
    if (lines.isEmpty) {
      readRows()
    }

    firstRow = lines.headOption
    firstRow
  }

  def getFirstKey: Option[String] = {
    getFirstRow match {
      case None => None
      case Some(r) => Some(r._1)
    }
  }

  def popFirstRow: Entry = {
    //lines.remove(0)
    lines.dequeue()
  }

}

object ChunkMergeActor {
  def props() =
    Props(classOf[ChunkMergeActor])
}


class ChunkMergeActor extends Actor {
  val CHUNK_ROWS = 960000000 / LINE_SIZE
  val init_min = intToString(127)

  def receive = {
    case MergeChunk(phase, index, indexList) =>

      indexList match {
        case x :: Nil => copy(phase, index, x)
        case x :: xs => merge(phase, index, indexList)
        case _ => println("something's wrong")
      }

      sender ! MergeChunkDone(phase, index)
  }

  def isChunkEmpty(chunks: Array[SortedChunk]): Boolean = {
    chunks.foldRight(true)((c, b) => {
      c.empty = c.getFirstRow.isEmpty
      c.empty && b
    })
  }

  def copy(phase: Int, index: Int, fileIndex: Int) = {
    println(s"merge phase $phase, index $index: copy $fileIndex")

    val fileList = getListOfFiles("merge/" + getIP, recursive = false)
      .filter(_.getName.startsWith("merge_%d_%d_".format(phase - 1, fileIndex))).toVector

    for (i <- fileList.indices) yield fileList(i).renameTo(new File("merge/%s/merge_%d_%d_%d".format(getIP, phase, index, i)))
  }

  def merge(phase: Int, index: Int, indexList: List[Int]): Unit = {
    println(s"merge phase $phase, index $index: " + indexList)

    var mergedList2 = mutable.Queue.empty[(String, String)]

    val init_chunks = indexList.map(new SortedChunk(phase, _)).toArray
    var chunks = init_chunks


    var partIndex = 0
    var fw = new FileWriter("merge/%s/merge_%d_%d_0".format(getIP, phase, index), false)
    var bw = new BufferedWriter(fw)

    while (!isChunkEmpty(chunks)) {

      var minKey = init_min
      var minIndex = 0

      chunks = chunks.filterNot(_.empty)
      //val (closechunks, b) = chunks.partition(_.empty)
      //chunks = b
      //closechunks.foreach(_.close())
      //closechunks.foreach(_.delete())

      for (i <- chunks.indices) {
        if (chunks(i).firstRow.get._1 < minKey) {
          minKey = chunks(i).firstRow.get._1
          minIndex = i
        }
      }

      //mergedList.append(chunks(minIndex).popFirstRow)
      mergedList2.enqueue(chunks(minIndex).popFirstRow)

      if (mergedList2.length >= CHUNK_ROWS) {
        println("writing part merge_%d_%d_%d".format(phase, index, partIndex))

        // write to file

        for ((k, v) <- mergedList2) {
          bw.append("%s%s\r\n".format(k, v))
        }


        /*
        @tailrec
        def writeResultFile(queue : mutable.Queue[(String, String)]) : Unit =
          if (queue.isEmpty) {
            bw.close()
            fw.close()
          }
          else {
            val (k,v) = queue.dequeue()
            bw.append("%s%s\r\n".format(k,v))
            writeResultFile(queue)
          }

        writeResultFile(mergedList2)
        */

        // close part file
        bw.close()
        fw.close()

        mergedList2.clear()

        println("Writing is end")

        // open up new part file
        partIndex = partIndex + 1
        fw = new FileWriter("merge/%s/merge_%d_%d_%d".format(getIP, phase, index, partIndex), false)
        bw = new BufferedWriter(fw)
      }
    }

    println("writing part merge_%d_%d_%d".format(phase, index, partIndex))


    for ((k, v) <- mergedList2) {
      bw.append("%s%s\r\n".format(k, v))
    }

    bw.close()
    fw.close()

    if(mergedList2.isEmpty)
      new File("merge_%d_%d_%d".format(phase,index,partIndex)).delete()

    mergedList2.clear()
    init_chunks.foreach(_.delete())


  }

  @tailrec
  private def mergeRec[T](a: List[T], b: List[T], acc: List[T])(implicit ev1: T => Ordered[T]): List[T] = (a, b) match {
    case (Nil, ys) => acc.reverse ++ ys
    case (xs, Nil) => acc.reverse ++ xs
    case (x :: xs, y :: ys) if x < y => mergeRec(xs, y :: ys, x :: acc)
    case (x :: xs, y :: ys) => mergeRec(x :: xs, ys, y :: acc)
  }

}