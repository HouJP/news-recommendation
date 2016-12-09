package com.houjp.recommendation.news

import com.houjp.common.{FileOpts, Log}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext}

object IO {

  /**
    * Load user information from disk, format: RDD[(user_id, user_vec)]
    *
    * @param sc     spark-context
    * @param date   date
    * @return       user information
    */
  def loadUser(sc: SparkContext, date: String): RDD[(String, String)] = {
    val fp = s"${com.houjp.recommendation.news_user_pt}/$date.user.txt"

    if (FileOpts.checkFile(fp)) {
      Log.log("INFO", s"Detected $fp")
      sc.textFile(fp).filter(_.trim.contains("\t")).map { s =>
        val Array(uid, info) = s.split("\t")
        (uid, info)
      }
    } else {
      Log.log("INFO", s"Do not detected $fp")
      sc.parallelize(Seq[(String, String)]())
    }
  }

  /**
    * Save user information to disk, format: RDD[(user_id, user_vec)]
    *
    * @param user user information
    * @param date date, format: yyyy-MM-DD
    */
  def saveUser(user: RDD[(String, String)], date: String): Unit = {
    val fp = s"${com.houjp.recommendation.news_user_pt}/$date.user.txt"

    user.map(e => s"${e._1}\t${e._2}").saveAsTextFile(fp)
  }

  /**
    * Load documents from disk, format: Seq[(document_id, Seq[(word, frequency)] ]
    * @param sc     spark-context
    * @param date   date
    * @return       documents
    */
  def loadDocs(sc: SparkContext, date: String): Seq[(String, Seq[(String, Double)])] = {
    val fp = s"${com.houjp.recommendation.news_doc_pt}/$date.doc.txt"

    // load docs from disk, and create a empty dataframe if file not exists
    val docs = if (FileOpts.checkFile(fp)) {
      Log.log("INFO", s"Detected $fp")
      sc.textFile(fp).map { s =>
        val Array(did, info_s) = s.split("\t")
        val info = info_s.split(",").map { kv =>
          val Array(k, v) = kv.split(":")
          (k, v.toDouble)
        }.toSeq
        (did, info)
      }
    } else {
      Log.log("INFO", s"Do not detected $fp")
      sc.parallelize(Seq[(String, Seq[(String, Double)])]())
    }

    docs.collect().toSeq
  }
}