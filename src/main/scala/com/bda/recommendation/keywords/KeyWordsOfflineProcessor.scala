package com.bda.recommendation.keywords

import com.bda.common.TimeOpts
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j.{Level, Logger}
import scopt.OptionParser

object KeyWordsOfflineProcessor {

  /** command line parameters */
  case class Params(date: String = "2015-07-23")

  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("aka").setLevel(Level.WARN)
    val default_params = Params()

    val parser = new OptionParser[Params]("") {
      head("KeyWordsOfflineProcessor", "1.0")
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
      .setAppName(s"Key-Words Recommendation OfflineProcessor(date=${p.date})")
      .set("spark.hadoop.validateOutputSpecs", "false")
    if (com.bda.recommendation.is_local) {
      conf.setMaster("local[4]")
    }
    val sc = new SparkContext(conf)
    val sql_c = new SQLContext(sc)

    // load docs and previous co-occur statistics
    val docs = IO.loadDocs(sc, sql_c, p.date)
    val co_occur = IO.loadCoOccurFile(sc, TimeOpts.calDate(p.date, -1))

    // update according to docs
    val new_co_occur = addCoOccur(sc, docs, co_occur)

    // save statitics on disk
    IO.saveCoOccurFile(new_co_occur, p.date)
  }

  def addCoOccur(sc: SparkContext,
                 docs: DataFrame,
                 co_occur: RDD[((String, String), Int)]): RDD[((String, String), Int)] = {
    val add = if (docs.columns.contains("kv")) {
      val regex = """\((.+)\)\[([0-9]+)\]""".r
      docs.select("kv").flatMap {
        case Row(kv: String) =>
          val kvs = kv.split(";").map {
            a_kv =>
              val regex(word, value) = a_kv
              (word, value.toInt)
          }
          kvs.indices.flatMap {
            i =>
              Range(i + 1, kvs.length).flatMap {
                j =>
                  val k1 = kvs(i)._1
                  val k2 = kvs(j)._1
                  val v1 = kvs(i)._2
                  val v2 = kvs(j)._2
                  Seq(((k1, k2), v1 * v2), ((k2, k1), v1 * v2))
              }
          }
      }
    } else {
      sc.parallelize(Seq[((String, String), Int)]())
    }
    (add ++ co_occur).reduceByKey(_+_)
  }
}