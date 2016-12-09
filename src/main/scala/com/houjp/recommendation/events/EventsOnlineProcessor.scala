package com.houjp.recommendation.events

import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, DataFrame, SQLContext}
import org.apache.spark.{SparkContext, SparkConf}

object EventsOnlineProcessor {
  var sc: SparkContext = _
  var sql_c: SQLContext = _

  def init(sc: SparkContext, sql_c: SQLContext, date: String): Unit = {
    this.sc = sc
    this.sql_c = sql_c

    val rdd = IO.loadRecDocs(sc, date).map{e => Row(e._1, e._2)}
    val schema = StructType(
      StructField("uid", StringType) ::
      StructField("rec_docs", StringType) :: Nil
    )
    sql_c.createDataFrame(rdd, schema).persist().registerTempTable("evt_rec")

    query("TEST")
  }

  def query(uid: String): String = {
    val rec_docs = sql_c.sql(s"select rec_docs from evt_rec where uid = '$uid'").collect().map {
      case Row(rec_docs: String) =>
        rec_docs
    }
    if (0 < rec_docs.length) {
      rec_docs(0)
    } else {
      ""
    }
  }
}