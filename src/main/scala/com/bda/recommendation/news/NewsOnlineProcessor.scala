package com.bda.recommendation.news

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import com.bda.common.{Log, VectorOpts}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StringType, StructField, StructType}

object NewsOnlineProcessor {

  var sc: SparkContext = _
  var sql_c: SQLContext = _
  var docs: Seq[(String, Seq[(String, Double)])] = _
  var user_prev: RDD[(String, String)] = _
  var user_cache = collection.mutable.Map[String, String]()

  def init(sc: SparkContext, sql_c: SQLContext): Unit = {
    this.sc = sc
    this.sql_c = sql_c

    // get date of previous day
    val calendar = Calendar.getInstance()
    calendar.add(Calendar.DATE, -1)
    val date_format = new SimpleDateFormat("yyyy-MM-dd")
    val date = date_format.format(calendar.getTime)
    Log.log("INFO", s"init_date($date)")

    this.docs = IO.loadDocs(sc, date)
    this.user_prev = IO.loadUser(sc, date)

    val user = user_prev.map{e => Row(e._1, e._2)}
    val schema = StructType(
      StructField("uid", StringType) ::
      StructField("vec", StringType) :: Nil
    )
    sql_c.createDataFrame(user, schema).persist().registerTempTable("user")

    query("TEST", "")
  }

  def destruct(): Unit = {
    val user_cache = this.user_cache

    // get date of previous day
    val calendar = Calendar.getInstance()
    calendar.add(Calendar.DATE, -1)
    val date_format = new SimpleDateFormat("yyyy-MM-dd")
    val date = date_format.format(calendar.getTime)
    Log.log("INFO", s"destruct_date($date)")

    val user_now = (user_prev.map { case (uid: String, vec: String) =>
      (uid, user_cache.getOrElse(uid, vec))
    } union sc.parallelize(user_cache.toSeq)).distinct()

    IO.saveUser(user_now, date)
  }

  def query(uid: String, kvs: String): String = {
    // user vector
    val user_vec = collection.mutable.Map[String, Double]()
    user_cache.contains(uid) match {
      case true =>
        user_cache.getOrElse(uid, "").split(",").filter(_.length > 0).foreach { kv =>
          val Array(k, vs) = kv.split(":")
          user_vec(k) = user_vec.getOrElse(k, 0.0D) + vs.toDouble
        }
      case false =>
        sql_c.sql(s"select vec from user where uid = '$uid'").collect().foreach {
          case Row(kvs: String) =>
            kvs.split(",").filter(_.length > 0).foreach { kv =>
              val Array(k, vs) = kv.split(":")
              user_vec(k) = user_vec.getOrElse(k, 0.0D) + vs.toDouble
            }
        }
    }
    // update user vector with current vector
    kvs.split(",").filter(_.length > 0).foreach { kv =>
      val Array(k, vs) = kv.split(":")
      user_vec(k) = user_vec.getOrElse(k, 0.0D) + vs.toDouble
    }
    user_cache(uid) = user_vec.map(e => s"${e._1}:${e._2}").mkString(",")
    // calculate <user, document> similarity
    val rec_docs = docs.map { case (did: String, d_vec: Seq[(String, Double)]) =>
      (did, VectorOpts.calCosSimilarity(user_vec.toMap, d_vec))
    }.sortBy(_._2).reverse.slice(0, com.bda.recommendation.news_top_k)
    // make an answer
    rec_docs.map(e => s"${e._1}:${e._2}").mkString(",")
  }
}