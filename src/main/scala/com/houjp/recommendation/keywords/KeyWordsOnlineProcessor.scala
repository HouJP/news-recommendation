package com.houjp.recommendation.keywords

import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import com.houjp.recommendation

import scala.collection.mutable

object KeyWordsOnlineProcessor {
  var sc: SparkContext = _
  var sql_c: SQLContext = _

  def init(sc: SparkContext, sql_c: SQLContext, date: String): Unit = {
    this.sc = sc
    this.sql_c = sql_c

    val rdd = IO.loadCoOccurFile(sc, date).map {
      case ((w1, w2), score) =>
        (w1, Seq((w2, score)))
    }.reduceByKey(_++_).map {
      case (w: String, cw: Seq[(String, Int)]) =>
        val rws = cw.sortBy(_._2).take(recommendation.kw_top_k).map {
          case (w: String, n: Int) =>
            s"$w:$n"
        }.mkString(",")
        Row(w, rws)
    }
    val schema = StructType(
      StructField("w", StringType) ::
      StructField("rws", StringType) :: Nil
    )
    sql_c.createDataFrame(rdd, schema).persist().registerTempTable("kw_rec")

    query("TEST")
  }

  def query(ws: String): String = {
    val ws_arr = ws.split(recommendation.kw_query_seperator)
    val rec_builder = mutable.ArrayBuilder.make[String]
    ws_arr.flatMap {
      w =>
        sql_c.sql(s"select rws from kw_rec where w = '$w'").collect()
    }.flatMap {
      case Row(rws: String) =>
        rws.split(",").map {
          rw =>
            val Array(w, n) = rw.split(":")
            (w, n.toInt)
        }
    }.sortBy(_._2).map(_._1).reverse.distinct.foreach {
      w =>
        if (!ws_arr.contains(w)) {
          rec_builder += w
        }
    }
    rec_builder.result().take(recommendation.kw_top_k).mkString(",")
  }
}