package local

import java.io._

import akka.actor._
import akka.routing._
import common.Constants._
import common.SharedTypes.{Partition, Sample}
import common.Utils._
import common._

import scala.annotation.tailrec
import scala.collection.mutable
import scala.util.Random

class LocalActor(args: Config) extends Actor {

  val masterAddr = args.addr

  lazy val inputFileList = Random.shuffle(args.input.toList.flatMap(x => getListOfFiles(x, recursive = false)))
  lazy val outputDir = args.output.toList

  val client = context.actorOf(Props(classOf[ClientManager], classOf[ClientHandler], self), "Client")

  // create the remote actor
  val master = context.actorSelection(s"akka.tcp://HelloRemoteSystem@$masterAddr/user/RemoteActor")

  // constants
  val MERGE_POOL = 10
  val SORT_POOL = 20
  val MERGE_LEVEL = 6

  var chunkComplete = 0

  var sendingComplete = 0

  var mPartition: Partition = List()

  var phaseQueue: mutable.Map[Int, mutable.Queue[Int]] = mutable.Map()
  var phaseCount: mutable.Map[Int, Int] = mutable.Map()
  var phaseDone: mutable.Map[Int, Int] = mutable.Map()
  var phaseTotal: Array[Int] = Array()

  // create router for chunk sort
  var chunkSortRouter = context.actorOf(
    BalancingPool(1).props(ChunkSortActor.props(client)),
    "ChunkSortRouter"
  )

  // create router for merge
  var chunkMergeRouter = context.actorOf(
    BalancingPool(1).props(ChunkMergeActor.props()),
    "ChunkMergeRouter"
  )

  override def preStart(): Unit = {
    //         for debug use
//            rearrangeChunks()
//            mergeChunks()

    println("connecting to master")
    master ! Establish
  }

  def receive = {
    case EstablishAck =>
      val masterAddress = parseIP(sender.path.address.toString)
      println("connected to master: " + masterAddress)

    case RequestSample =>
      println("master requested sample")
      master ! SubmitSample(createSample)
      println("sample sent")

    case SubmitSampleAck =>
      println("master got sample. waiting for further request.")

    case NotifyPartition(p) =>
      println("\nslave received Partition information")
      client ! NotifyPartition(p)
      mPartition = p
      chunkSortRouter ! AdjustPoolSize(SORT_POOL - 1) //min(Runtime.getRuntime.availableProcessors, fileList.size) - 1)
      println(mPartition)
      sortChunks()

    case SortChunkDone =>
      chunkComplete = chunkComplete + 1
      //println("Chunk sort done: " + chunkComplete)

      if (chunkComplete == inputFileList.size) {
        master ! SortComplete
        println("Chunk sort complete")
        chunkSortRouter ! Broadcast(PoisonPill)
      }

    case FinishSort(filepath) =>
      println(s"Finish Sort : $filepath")

    case FinishSendingSorted(file, remote) =>
      sendingComplete = sendingComplete + 1
      //println(s"Finish sending {$file} to {$remote}")
      if (sendingComplete == (mPartition.size - 1) * inputFileList.size) {
        master ! FinishSending(sendingComplete)
        println("sending finish")
      }

    case StartMerge =>
      deleteDirectory("temp/" + Utils.getIP)

      println("initiate merge")
      chunkMergeRouter ! AdjustPoolSize(MERGE_POOL - 1)
      rearrangeChunks()
      mergeChunks()

    case MergeChunkDone(p, i) =>
      phaseDone(p) = phaseDone(p) + 1
      println(s"merge phase $p, index $i done")

      // done case
      if (phaseTotal(p) == phaseDone(p) && phaseTotal(p) == 1) {
        println("merge done")

        println("split final results")
        // split final merged files
        splitMergedFile(p, "merge/" + getIP, outputDir.head, CHUNK_SIZE)


        // now close myself and notify to master
        println("all done! bye~")
        master ! FinishMerge
      }

      // remaining last phase
      else if (phaseTotal(p) == phaseDone(p) && phaseTotal(p) % MERGE_LEVEL > 0) {
        phaseQueue(p).enqueue(i)

        chunkMergeRouter ! MergeChunk(p + 1, phaseCount(p + 1), dequeueAll(phaseQueue(p)))
        phaseCount(p + 1) = phaseCount(p + 1) + 1
      }

      // normal case
      else {
        phaseQueue(p).enqueue(i)

        if (phaseQueue(p).nonEmpty && phaseQueue(p).size >= MERGE_LEVEL) {
          chunkMergeRouter ! MergeChunk(p + 1, phaseCount(p + 1), dequeueFor(phaseQueue(p), MERGE_LEVEL))
          phaseCount(p + 1) = phaseCount(p + 1) + 1
        }
      }

    case SlaveTerminate =>
      context.system.shutdown()

    case msg: String =>
      println(s"LocalActor received message: '$msg'")

    case Message(msg) => println(s"Message : $msg")

    case x =>
      println(s"Unknown response : $x")

  }


  def dequeueAll(q: mutable.Queue[Int]): List[Int] = {
    val l = q.toList
    q.clear()
    l
  }

  def dequeueFor(q: mutable.Queue[Int], n: Int): List[Int] = {
    @tailrec
    def dequeueHelper(q: mutable.Queue[Int], n: Int, acc: List[Int]): List[Int] = {
      if (n == 0) acc
      else dequeueHelper(q, n - 1, acc :+ q.dequeue())
    }

    if (q.isEmpty) List()
    else dequeueHelper(q, n, List())
  }

  def createSample: Sample = {
//    println(inputFileList)

    val sampleRows = 1
    val chunkRows = CHUNK_SIZE / LINE_SIZE
    val totalSampleRows = MAXIMUM_PAYLOAD / KEY_SIZE - 1
    val n = chunkRows - sampleRows + 1
    val r = Random.nextInt(n)
    var keyList = List[String]()

    val byteBuffer = new Array[Byte](sampleRows * LINE_SIZE)

    for(f <- inputFileList if keyList.length != totalSampleRows) {
      val randomAccessFile = new RandomAccessFile(f, "r")

      randomAccessFile.seek(r * LINE_SIZE)
      randomAccessFile.read(byteBuffer)
      val rawString = new String(byteBuffer)
      val split = rawString.split(System.getProperty("line.separator"))

      split.foreach(line => {
        keyList = line.take(10) :: keyList
      })

      randomAccessFile.close()
    }

    keyList.reverse
  }

  def sortChunks(): Unit = {
    //println(inputFileList)

    // distribute work to children
    inputFileList.foreach(f => {
      chunkSortRouter ! SortChunk(mPartition, f)
    })
  }

  def mergeChunks() = {
    val inputFiles = getListOfFiles("merge/" + getIP, recursive = false).filter(_.getName.startsWith("merge_0_"))
    //    println("merge list: " + inputFiles)

    val bufferSize = 64000000 // 64MB

    def parseMergeFile(f: File): Int = {
      f.getName.split("_")(2).toInt
    }

    @tailrec
    def prepPhase1(l: List[File], acc: List[List[Int]], size: Long): List[List[Int]] = l match {
      case Nil => acc
      case x :: xs if size == 0 => prepPhase1(xs, List(parseMergeFile(x)) :: acc, x.length)
      case x :: xs if (size + x.length) < bufferSize => prepPhase1(xs, (parseMergeFile(x) :: acc.head) :: acc.tail, size + x.length)
      case x :: xs => prepPhase1(xs, List(parseMergeFile(x)) :: acc, x.length)
    }

    // distribute work
    val workList = prepPhase1(inputFiles, List(), 0)

    @tailrec
    def calcPhase(n: Int, acc: Array[Int]): Array[Int] = {
      if (n == 0) acc :+ 1
      else if (n == 1) acc :+ 1
      else if (n < MERGE_LEVEL) if (MERGE_LEVEL == 2) acc :+ n else calcPhase(n / MERGE_LEVEL, acc :+ n)
      else if ((n % MERGE_LEVEL) != 0) calcPhase(n / MERGE_LEVEL + 1, acc :+ n)
      else calcPhase(n / MERGE_LEVEL, acc :+ n)
    }

    phaseTotal = calcPhase(workList.length, Array(inputFiles.length))

    phaseCount(0) = inputFiles.length
    phaseDone(0) = inputFiles.length

    for (i <- 1 to phaseTotal.length) {
      phaseCount(i) = 0
      phaseDone(i) = 0
      phaseQueue(i) = mutable.Queue[Int]()
    }

    println("total phases: " + phaseTotal.toList)

    // start phase 1
    println("starting phase 1")

    var index = 0

    for (l <- workList) {
      chunkMergeRouter ! MergeChunk(1, index, l)
      index = index + 1
    }
  }

  def rearrangeChunks() = {
    println("arranging chunks for sort")

    val inputFiles = getListOfFiles("fromSlave/" + Utils.getIP, recursive = false)
    //    println("received list: " + inputFiles)

    //     these two lines are for debug use
//            val inputFiles = getListOfFiles("sorted/", recursive = false)
    //        println("received list: " + inputFiles)

    val dir = new File("merge/" + Utils.getIP)
    if (!dir.exists())
      dir.mkdirs()

    var index = 0

    for (f <- inputFiles) {
      f.renameTo(new File("merge/%s/merge_0_%d_0" format(Utils.getIP, index)))
      index = index + 1
    }
  }

  def splitMergedFile(lastPhase: Int, mergeDir: String, destDir: String, chunkSize: Long) = {
    val dir = new File(destDir)
    if (!dir.exists())
      dir.mkdirs()

    val parts = getListOfFiles(mergeDir, recursive = false)
      .filter(_.getName.startsWith("merge_%d_".format(lastPhase)))
      .toVector.sortBy(_.getName.split("_", 4)(3))

    var index = 0

    for (f <- parts) {
      val in = new BufferedInputStream(new FileInputStream(f))
      val partsNum = (f.length / CHUNK_SIZE).toInt

      for (i <- 0 until partsNum) {
        val out = new BufferedOutputStream(new FileOutputStream("%s/partition_%d".format(destDir, index)))

        for (i <- 0 until CHUNK_SIZE) {
          out.write(in.read)
        }

        out.close()
        index = index + 1
      }

      // loop for the last chunk (which may be smaller than the chunk size)
      if (f.length % CHUNK_SIZE != 0) {
        val out = new BufferedOutputStream(new FileOutputStream("%s/partition_%d".format(destDir, index)))

        // write the rest of the file
        var b = in.read
        while (b != -1) {
          out.write(b)
          b = in.read
        }

        // close the file
        out.close()
      }

      in.close()
      f.delete()
    }
  }
}
