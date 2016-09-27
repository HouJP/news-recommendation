package com.bda.recommendation.news.service

import akka.actor.{ActorSystem, Props}
import akka.io.IO
import com.bda.recommendation.news.NewsOnlineProcessor
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import scopt.OptionParser
import spray.can.Http

object Boot {

  /** command line parameters */
  case class Params()

  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("aka").setLevel(Level.WARN)
    val default_params = Params()

    val parser = new OptionParser[Params]("") {
      head("News Recommendation", "1.0")
      help("help").text("prints this usage text")
    }

    parser.parse(args, default_params) match {
      case Some(params) => run(params)
      case None => System.exit(1)
    }
  }

  def run(p: Params): Unit = {
    val conf = new SparkConf()
      .setAppName(s"News Recommendation OnlineProcessor")
      .set("spark.hadoop.validateOutputSpecs", "false")
    if (com.bda.recommendation.is_local) {
      conf.setMaster("local[4]")
    }
    val sc = new SparkContext(conf)
    val sql_c = new SQLContext(sc)

    NewsOnlineProcessor.init(sc, sql_c)

    implicit val actorSystem = ActorSystem("BDA")
    val service = actorSystem.actorOf(Props[ServiceActor], "News-Recommendation")
    IO(Http) ! Http.Bind(service,
      interface = com.bda.recommendation.host,
      port = com.bda.recommendation.port)
  }
}