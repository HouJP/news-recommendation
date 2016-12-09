package com.houjp.recommendation.service

import akka.actor.{Props, ActorSystem}
import akka.io.IO
import com.houjp.recommendation
import com.houjp.recommendation.events.EventsOnlineProcessor
import com.houjp.recommendation.keywords.KeyWordsOnlineProcessor
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}
import scopt.OptionParser
import spray.can.Http

object Boot {

  /** command line parameters */
  case class Params(date: String = "2015-12-23")

  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("aka").setLevel(Level.WARN)
    val default_params = Params()

    val parser = new OptionParser[Params]("") {
      head("", "1.0")
      opt[String]("date")
        .text("date")
        .action { (x, c) => c.copy(date = x) }
      help("help").text("prints this usage text")
    }

    parser.parse(args, default_params) match {
      case Some(params) => run(params)
      case None => System.exit(1)
    }
  }

  def run(p: Params): Unit = {
    val conf = new SparkConf()
      .setAppName(s"Recommendation OnlineProcessor")
      .set("spark.hadoop.validateOutputSpecs", "false")
    if (com.houjp.recommendation.is_local) {
      conf.setMaster("local[4]")
    }
    val sc = new SparkContext(conf)
    val sql_c = new SQLContext(sc)

    KeyWordsOnlineProcessor.init(sc, sql_c, p.date)

    EventsOnlineProcessor.init(sc, sql_c, p.date)

    implicit val actorSystem = ActorSystem("BDA")
    val service = actorSystem.actorOf(Props[ServiceActor], "Recommendation")
    IO(Http) ! Http.Bind(service, interface = recommendation.host, port = recommendation.port)
  }
}