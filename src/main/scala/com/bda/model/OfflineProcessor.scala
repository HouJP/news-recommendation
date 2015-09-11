package com.bda.model

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext, Row}
import org.apache.spark.{Logging, SparkConf, SparkContext}
import com.bda.util.Log

class OfflineProcessor(pre_time: String, time: String, data: String, has_docs: Boolean, has_user: Boolean) {
  def run(): Unit = {
    //val spark_context = new SparkContext(new SparkConf().setAppName("BDA-NewsRecommendation-OfflineProcessor").setMaster("local"))
    val spark_context = new SparkContext(new SparkConf().setAppName("BDA-NewsRecommendation-OfflineProcessor"))
    val sql_context = new SQLContext(spark_context)

    /* load pre docs num */
    val pre_docs_num = News.loadDocsNum(spark_context, data + Conf.docs_num_dir + "/" + pre_time).collect()(0)

    /* load pre words df */
    val pre_words_df = News.loadWordsDF(spark_context, data + Conf.words_df_dir + "/" + pre_time)

    /* load pre user vec */
    val pre_user_vec = User.loadUserVec(spark_context, data + Conf.user_vec_dir + "/" + pre_time)

    /* load docs */
    val docs_df: DataFrame = News.loadDocs(spark_context, sql_context, data + Conf.docs_dir + "/" + time + ".txt", has_docs)

    /* load user view history */
    val user_nids = User.loadHistory(spark_context, data + Conf.user_info_dir + "/" + time + ".txt", has_user)

    /* update docs num */
    val new_docs_num = News.updateDocsNum(pre_docs_num, docs_df)

    /* update words df */
    val new_words_df = News.updateWordsDF(pre_words_df, docs_df)

    /* cal words idf */
    val words_idf = News.calWordsIDF(new_words_df, new_docs_num).cache()
    val words_idf_maped = words_idf.collectAsMap().toMap

    /* get docs key words */
    val docs_kw = News.calKeyWords(docs_df)

    /* get docs normalized vector */
    val docs_vector = News.calVector(docs_kw, words_idf_maped)

    /* calculate user vector */
    val user_vector = User.calVector(user_nids, docs_vector)

    /* update user vector */
    val new_user_vec = User.updateUserVec(pre_user_vec, user_vector)

    /* save new docs num */
    spark_context.parallelize(Seq(new_docs_num)).saveAsObjectFile(data + Conf.docs_num_dir + "/" + time)

    /* save new words df */
    new_words_df.saveAsObjectFile(data + Conf.words_df_dir + "/" + time)

    /* save new user vec */
    new_user_vec.saveAsObjectFile(data + Conf.user_vec_dir + "/" + time)

    /* save words idf */
    words_idf.saveAsObjectFile(data + Conf.words_idf_dir + "/" + time)

    spark_context.stop()
  }
}