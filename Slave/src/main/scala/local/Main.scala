package local

import java.io.File

import akka.actor.{ActorSystem, Props}


object Main {

  def main(args: Array[String]) {
    parser.parse(args, Config()) match {
      case Some(config) =>
        println("Input arguments: " + config)

        val system = ActorSystem("LocalSystem")
        val localActor = system.actorOf(Props(new LocalActor(config)), name = "LocalActor") // the local actor
        val server = system.actorOf(Props(classOf[EchoManager], classOf[SimpleEchoHandler]), "simple") //Props(classOf[EchoManager], classOf[SimpleEchoHandler]), "simple")

      case None =>
        // arguments are bad, error message will have been displayed
//        sys.exit(1)

    }
  }

  val parser = new scopt.OptionParser[Config]("slave.jar") {
    head("SD Clustered Sorting - Slave")
    arg[String]("<master IP:port>") action { (x, c) =>
      c.copy(addr = x)
    } text ("address of master")
    opt[Seq[String]]('I', "input") required() valueName ("<input1>, <input2>...") action { (x, c) =>
      c.copy(input = x)
    } validate { x =>
      if (x.forall(new File(_).exists())) success else failure("One of input dir does not exists")
    } text ("list of input directories")
    opt[Seq[String]]('O', "output") required() valueName ("<output1>, <output2>...") action { (x, c) =>
      c.copy(output = x)
    } text ("list of output directories")
    help("help") text ("prints this usage text")

    override def showUsageOnError = true
  }

}


case class Config(addr: String = "", input: Seq[String] = Seq(), output: Seq[String] = Seq())