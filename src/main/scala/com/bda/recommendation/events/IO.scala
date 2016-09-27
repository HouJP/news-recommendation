package com.bda.recommendation.events

import com.bda.common.{Log, FileOpts}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, DataFrame, SQLContext}

object IO {

  def loadDocs(sc: SparkContext, sql_c: SQLContext, date: String): Seq[(String, Seq[String])] = {
    val fp = s"${com.bda.recommendation.evt_docs_pt}/$date.txt"

    // load docs from disk, and create a empty dataframe if file not exists
    val docs = if (FileOpts.checkFile(fp)) {
      Log.log("INFO", s"Detected $fp")
      sql_c.read.json(fp)
    } else {
      Log.log("INFO", s"Do not detected $fp")
      val docs = sc.parallelize(Seq[(String, String)]()).map(e => Row(e._1, e._2))
      val schema = StructType(
        StructField("event_id", StringType) ::
        StructField("keyword", StringType) :: Nil
      )
      sql_c.createDataFrame(docs, schema)
    }

    // select specified columns, and collected as sequence
    docs.select("event_id", "keyword").map {
      case Row(eid: String, kws: String) =>
        (eid, kws.split(" ").filter(! _.isEmpty).toSeq)
    }.collect()
  }

  def loadUserView(sc: SparkContext, date: String): RDD[(String, String)] = {
    val fp = s"${com.bda.recommendation.evt_user_pt}/$date.txt"

    if (FileOpts.checkFile(fp)) {
      Log.log("INFO", s"Detected $fp")
      sc.textFile(fp).map(_.split("\t")).filter(_.length == 3).map {
        case Array(uid, eid, kws) =>
          (uid, kws)
      }
    } else {
      Log.log("INFO", s"Do not detected $fp")
      sc.parallelize(Seq[(String, String)]())
    }
  }

  def loadUserInfo(sc: SparkContext, date: String): RDD[(String, Map[String, Double])] = {
    val fp = s"${com.bda.recommendation.evt_out_pt}/$date.user.obj"

    if (FileOpts.checkFile(fp)) {
      Log.log("INFO", s"Detected $fp")
      sc.objectFile[(String, Map[String, Double])](fp)
    } else {
      Log.log("INFO", s"Do not detected $fp")
      sc.parallelize(Seq[(String, Map[String, Double])]())
    }
  }

  def saveUserInfo(user_info: RDD[(String, Map[String, Double])], date: String): Unit = {
    user_info.saveAsObjectFile(s"${com.bda.recommendation.evt_out_pt}/$date.user.obj")
  }

  def loadRecDocs(sc: SparkContext, date: String): RDD[(String, String)] = {
    val fp = s"${com.bda.recommendation.evt_out_pt}/$date.docs.obj"

    if (FileOpts.checkFile(fp)) {
      Log.log("INFO", s"Detected $fp")
      sc.objectFile[(String, String)](fp)
    } else {
      Log.log("INFO", s"Do not detected $fp")
      sc.parallelize(Seq[(String, String)]())
    }
  }

  def saveRecDocs(rec_docs: RDD[(String, String)], date: String): Unit = {
    rec_docs.saveAsObjectFile(s"${com.bda.recommendation.evt_out_pt}/$date.docs.obj")
  }
}