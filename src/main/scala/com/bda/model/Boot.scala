package com.bda.model

import akka.actor._
import akka.io.IO
import spray.can.Http
import scopt._
import com.bda.util.Log

object Boot {

  def initProcess(time: String, data: String): Unit = {
    new InitProcessor(time, data).run()
  }

  def offlineProcess(pre_time: String, time:String, data: String, has_docs: Boolean, has_user: Boolean): Unit = {
    new OfflineProcessor(pre_time, time, data, has_docs, has_user).run()
  }

  def onlineProcess(time: String, data: String, host: String, port: Int): Unit = {
    /* init online processor */
    OnlineProcessor.init(time, data)

    /* boot online processor service */
    OnlineProcessor.run(host, port)

    /* test */
    //OnlineProcessor.query("1000000142", "01793009222687981568", "我们 交通 我们", "news_doc", 10)
  }

  def main(args: Array[String]) {
    val parser = new OptionParser[Para]("NewsRecommendation") {
      head("NewsRecommendation", "1.0")
      opt[String]('p', "pre_time") action { (x, c) =>
        c.copy(pre_time = x) } text("pre time is the date of last update")
      opt[String]('t', "time") action { (x, c) =>
        c.copy(time = x) } text("time is the date of new data")
      opt[String]('d', "data") action { (x, c) =>
        c.copy(data = x) } text("data is the directory of data")
      opt[String]('b', "boot") action { (x, c) =>
        c.copy(boot = x) } validate { x => x match {
          case "init" => success
          case "online" => success
          case "offline" => success
          case _ => failure("Value <boot> must be [init|offline|online]")
        }
      } text("boot is the command to run: [init|online|offline]")
      opt[String]('h', "host") action { (x, c) =>
        c.copy(host = x) } text("host is the host of service")
      opt[Int]('p', "port") action { (x, c) =>
        c.copy(port = x) } text("port is the port of service")
      opt[Boolean]('o', "has_docs") action { (x, c) =>
        c.copy(has_docs = x) } text("has_docs is the flag of docs")
      opt[Boolean]('u', "has_user") action { (x, c) =>
        c.copy(has_user = x) } text("has_user is the flag of user")
    }
    parser.parse(args, Para()) match {
      case Some(config) =>
        config.boot match {
          case "init" =>
            Log.log("DEBUG", "init ...")
            initProcess(config.pre_time, config.data)
          case "offline" =>
            Log.log("DEBUG", "offline ...")
            offlineProcess(config.pre_time, config.time, config.data, config.has_docs, config.has_user)
          case "online" =>
            Log.log("DEBUG", "online ...")
            onlineProcess(config.time, config.data, config.host, config.port)
        }
      case None =>
        Log.log("ERROR", "parameter error!")
    }
  }
}