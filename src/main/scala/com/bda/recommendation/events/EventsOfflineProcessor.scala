package com.bda.recommendation.events

import com.bda.common.{VectorOpts, TimeOpts}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j.{Level, Logger}
import scopt.OptionParser
import com.bda.recommendation

import scala.collection.mutable

object EventsOfflineProcessor {
  case class Params(date: String = "2015-12-23")

  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("aka").setLevel(Level.WARN)
    val default_params = Params()

    val parser = new OptionParser[Params]("") {
      head("EventsOfflineProcessor", "1.0")
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
    // init spark context
    val conf = new SparkConf()
      .setAppName(s"Events Recommendation OfflineProcessor(date=${p.date})")
      .set("spark.hadoop.validateOutputSpecs", "false")
    if (com.bda.recommendation.is_local) {
      conf.setMaster("local[4]")
    }
    val sc = new SparkContext(conf)
    val sql_c = new SQLContext(sc)

    val docs_info = IO.loadDocs(sc, sql_c, p.date)
    val user_view = IO.loadUserView(sc, p.date)
    val user_info = IO.loadUserInfo(sc, TimeOpts.calDate(p.date, -1))

    // update according to history info of user
    val new_user_info = addUserInfo(sc, user_view, user_info)

    // calculate recommendations for user
    val rec_docs = recEventsForUser(new_user_info, docs_info)

    // save results on disk
    IO.saveUserInfo(new_user_info, p.date)
    IO.saveRecDocs(rec_docs, p.date)
  }

  def addUserInfo(sc: SparkContext,
                  user_view: RDD[(String, String)],
                  user_info: RDD[(String, Map[String, Int])]): RDD[(String, Map[String, Int])] = {

    val new_user_info = user_view.flatMap {
      case (uid: String, kws: String) =>
        kws.split(" ").filter(! _.isEmpty).map(e => ((uid, e), 1))
    }.reduceByKey(_+_).map {
      case ((uid: String, w: String), n: Int) =>
        (uid, Seq((w, n)))
    }.reduceByKey(_++_).map {
      case (uid: String, cnt: Seq[(String, Int)]) =>
        (uid, cnt.toMap)
    }

    (new_user_info ++ user_info).reduceByKey {
      (cnt_1: Map[String, Int], cnt_2: Map[String, Int]) =>
        val cnt = mutable.Map[String, Int]()
        cnt_1.foreach {
          case (w: String, n: Int) =>
            cnt(w) = cnt.getOrElse(w, 0) + n
        }
        cnt_2.foreach {
          case (w: String, n: Int) =>
            cnt(w) = cnt.getOrElse(w, 0) + n
        }
        cnt.toMap
    }
  }

  def recEventsForUser(user_info: RDD[(String, Map[String, Int])],
                       docs_info: Seq[(String, Seq[String])]): RDD[(String, String)] = {

    user_info.map {
      case (uid: String, cnt: Map[String, Int]) =>
        val len_user = VectorOpts.calLength(cnt)
        // println(s"uid=$uid,len_user=$len_user")
        val rec_docs = docs_info.map {
          case (eid: String, kws: Seq[String]) =>
            val len_doc = math.sqrt(kws.length)
            val dot_product = kws.map {
              w =>
                cnt.getOrElse(w, 0)
            }.sum
            // println(s"eid=$eid,len_doc=$len_doc,dot_product=$dot_product")
            val cos = dot_product / len_doc / len_user
            (eid, cos)
        }.sortBy(_._2).take(recommendation.evt_top_k).map(e => s"${e._1}:${e._2}")
        (uid, rec_docs.mkString(","))
    }
  }
}