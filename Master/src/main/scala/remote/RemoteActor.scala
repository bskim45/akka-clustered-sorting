package remote

import akka.actor._
import common.SharedTypes.{Partition, Sample}
import common._


class RemoteActor(slaveNum: Int) extends Actor {
  var slaves = List[ActorRef]()
  var sampleKeys = List[String]()
  var nrOfReceivedSample = 0
  var finishSend = 0
  var finishSort = 0
  var finishMerge = 0
  var startTime = 0: Long


  def parseActorIP(a: ActorRef): String = {
    Utils.parseIP(a.path.address.toString)
  }

  def receive = {
    case Start =>
      println("waiting for " + slaveNum + " slaves")

    case Establish => handleEstablish

    case SubmitSample(sample) => handinSample(sample)

    case FinishSending(fn) =>
      finishSend = finishSend + 1
      println("slave %s : finish sending %d sorted files to all slaves".format(parseActorIP(sender), fn))
      if (finishSend == slaveNum) {
        val time = (System.currentTimeMillis() - startTime) * 0.001
        println("finish all savle-to-slave transport")
        println("the total sending time is : %.2f seconds".format(time))
        slaves.foreach(_ ! StartMerge)
      }

    case FinishMerge =>
      finishMerge = finishMerge + 1
      println("slave %s : merge finish".format(parseActorIP(sender)))
      if (finishMerge == slaveNum) {
        val time = (System.currentTimeMillis() - startTime) * 0.001
        println("finish all merge")
        println("the total time is : %.2f seconds".format(time))

        // kill slaves
        slaves.foreach(_ ! SlaveTerminate)

        // close myself
        println("bye")
        context.system.shutdown()

      }

    case SortComplete =>
      finishSort = finishSort + 1
      if (finishSort == slaveNum) {
        val sortingTime = (System.currentTimeMillis() - startTime) * 0.001
        println("all slaves finish sort")
        println("the sort time is : %.2f seconds".format(sortingTime))
        //        slaves.foreach(_ ! StartMerge)
      }

    case KeepAlive =>
    // do nothing

    case msg: String =>
      println(s"RemoteActor received message '$msg'")
      sender ! "Hello from the RemoteActor"

    case _ =>
      println("Unknown response")
  }

  def handleEstablish = {
    if (slaves.size == slaveNum) {
      sender ! Message("connection exceeded. terminate.")
      sender ! Kill
    }

    slaves = sender :: slaves

    val slaveAddress = parseActorIP(sender)
    println("slave %d: %s connected".format(slaves.size, slaveAddress))

    sender ! EstablishAck

    if (slaves.size == slaveNum) {
      startTime = System.currentTimeMillis()
      println("all slaves connected. requesting samples")
      slaves.foreach(s => s ! RequestSample)

    }
  }

  def handinSample(sample: Sample) = {
    println("sample from " + parseActorIP(sender) + " arrived")
    sender ! SubmitSampleAck

    val node = parseActorIP(sender)
    //println(s"sample($node)")

    sampleKeys = sample ::: sampleKeys
    nrOfReceivedSample += 1

    if (nrOfReceivedSample < slaveNum) {}

    else {

      val sortedSample = sampleKeys.sortWith(_ < _)

      var i = 0

      /*
      sampleKeys.foreach(s => {
        println(i + ": " + s)
        i += 1
      })


      i = 0
      sortedSample.foreach(s => {
        println(i + ": " + s)
        i += 1
      })
      */

      i = sortedSample.length

      element = i / slaveNum
      //println(s"element($element)")

      val PartitionList: Partition = makePartition(0, sortedSample)
      //println(PartitionList)
      //PartitionList.foreach({
      //case (ip, (key1, key2)) =>
      //println(key1.map(c => c.toInt))
      //println(key2.map(c => c.toInt))
      //})

      slaves foreach (x => x ! NotifyPartition(PartitionList))
    }
  }

  val min = intToString(0)
  val max = intToString(127)
  var element = 0

  def intToString(n: Int): String = {
    val c = n.toChar
    def iter(i: Int): String = {
      if (i > 1) c +: iter(i - 1)
      else c.toString
    }
    iter(10)
  }

  //n : partionNumber
  //fix : first element and last element should contain minimum string and maximum string
  def makePartition(n: Int, sorted: List[String]): Partition = {
    if (slaveNum == 1) List((parseActorIP(slaves.head), (min, max)))
    else if (n < slaveNum - 1) {
      val (xs, ys) = sorted.splitAt(element)
      val l = xs.length
      if (n == 0) (parseActorIP(slaves(n)), (min, ys.head)) :: makePartition(n + 1, ys)
      else (parseActorIP(slaves(n)), (xs.head, ys.head)) :: makePartition(n + 1, ys)
    }
    else if (n == slaveNum - 1) List((parseActorIP(slaves(n)), (sorted.head, max)))
    else List()
  }

}


