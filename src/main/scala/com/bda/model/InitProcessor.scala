package com.bda.model

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext, Row}
import org.apache.spark.{Logging, SparkConf, SparkContext}
import com.bda.util.Log

class InitProcessor(time: String, data: String) {
  def run(): Unit = {
    //val sc = new SparkContext(new SparkConf().setAppName("BDA-NewsRecommendation-InitProcessor").setMaster("local"))
    val sc = new SparkContext(new SparkConf().setAppName("BDA-NewsRecommendation-InitProcessor"))

    val docs_num: RDD[Int] = sc.parallelize[Int](Seq[Int](0))
    docs_num.saveAsObjectFile(data + Conf.docs_num_dir + "/" + time)

    val words_df: RDD[(String, Int)] = sc.parallelize[(String, Int)](Seq[(String, Int)]())
    words_df.saveAsObjectFile(data + Conf.words_df_dir + "/" + time)

    val user_vec: RDD[(String, Map[String, Double])] = sc.parallelize[(String, Map[String, Double])](Seq[(String, Map[String, Double])]())
    user_vec.saveAsObjectFile(data + Conf.user_vec_dir + "/" + time)

    sc.stop()
  }
}