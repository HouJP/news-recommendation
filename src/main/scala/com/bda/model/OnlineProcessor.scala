package com.bda.model

import akka.actor.{Props, ActorSystem}
import akka.io.IO
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import com.bda.util.Log
import spray.can.Http

object OnlineProcessor {
  //val spark_context = new SparkContext(new SparkConf().setAppName("BDA-NewsRecommendation-OnlineProcessor").setMaster("local"))
  val spark_context = new SparkContext(new SparkConf().setAppName("BDA-NewsRecommendation-OnlineProcessor"))
  val sql_context = new SQLContext(spark_context)

  var time: String = _
  var data: String = _

  var words_idf: Map[String, Double] = _
  var news_vecs: collection.mutable.ArrayBuffer[Array[(String, Map[String, Double])]] = _
  var oversea_news_vecs: collection.mutable.ArrayBuffer[Array[(String, Map[String, Double])]] = _
  var news_titles: collection.mutable.ArrayBuffer[Map[String, String]] = _
  var oversea_news_titles: collection.mutable.ArrayBuffer[Map[String, String]] = _
  var user_vecs: Map[String, Map[String, Double]] = _

  def init(time: String, data: String): Unit = {
    OnlineProcessor.time = time
    OnlineProcessor.data = data

    /* init news_vecs */
    OnlineProcessor.news_vecs = collection.mutable.ArrayBuffer[Array[(String, Map[String, Double])]]()
    for (i <- 0 until Conf.news_doc_slices_num) {
      OnlineProcessor.news_vecs.append(Array[(String, Map[String, Double])]())
    }
    OnlineProcessor.oversea_news_vecs = collection.mutable.ArrayBuffer[Array[(String, Map[String, Double])]]()
    for (i <- 0 until Conf.news_doc_slices_num) {
      OnlineProcessor.oversea_news_vecs.append(Array[(String, Map[String, Double])]())
    }

    /* init news_titiles */
    OnlineProcessor.news_titles = collection.mutable.ArrayBuffer[Map[String, String]]()
    for (i <- 0 until Conf.news_doc_slices_num) {
      OnlineProcessor.news_titles.append(Map[String, String]())
    }
    OnlineProcessor.oversea_news_titles = collection.mutable.ArrayBuffer[Map[String, String]]()
    for (i <- 0 until Conf.news_doc_slices_num) {
      OnlineProcessor.oversea_news_titles.append(Map[String, String]())
    }

    /* load words idf */
    OnlineProcessor.words_idf = News.loadWordsIDF(spark_context, data + "/" + Conf.words_idf_dir + "/" + time).collectAsMap().toMap
    //Log.log("DEBUG", "words_idf: " + OnlineProcessor.words_idf)

    /* load news slices */
    for (typ <- Conf.news_doc_type) {
      for (id <- 0 until Conf.news_doc_slices_num) {
        update(typ, id)
      }
    }

    /* load user vecs */
    OnlineProcessor.user_vecs = User.loadUserVec(spark_context, data + "/" + Conf.user_vec_dir + "/" + time).collectAsMap().toMap
    //Log.log("DEBUG", "user_vecs: " + user_vecs)
  }

  def update(typ: String, id: Int): Unit = {
    if (id < 0 || id >= Conf.news_doc_slices_num) {
      return
    }
    Log.log("INFO", "udpate " + typ + " " + id + " ...")

    /* load news */
    val news_df: DataFrame = News.loadDocs(spark_context, sql_context, data + "/" + typ + "/" + Conf.news_doc_fn_pre + id.toString + Conf.news_doc_fn_suf, true).persist()

    /* get news key words */
    val news_kw = News.calKeyWords(news_df)

    /* cal news normalized vector */
    val news_vector: Array[(String, Map[String, Double])] = News.calVector(news_kw, words_idf).collect()

    /* get news title */
    val news_title = News.getTitle(news_df).collectAsMap().toMap

    /* update news vecs and title */
    if ("news_doc" == typ) {
      OnlineProcessor.news_vecs(id) = news_vector
      OnlineProcessor.news_titles(id) = news_title
    } else {
      OnlineProcessor.oversea_news_vecs(id) = news_vector
      OnlineProcessor.oversea_news_titles(id) = news_title
    }

    news_df.unpersist()

    Log.log("INFO", "udpate " + typ + " " + id + " done.")
  }

  def query(uid: String, nid: String, content: String, typ: String, topK: Int): Seq[(String, Double)] = {
    val id = util.Random.nextInt(Conf.news_doc_slices_num)

    querySlice(uid, nid, content, typ, id, topK)
  }

  def querySlice(uid: String, nid: String, content: String, typ: String, id: Int, topK: Int): Seq[(String, Double)] = {
    Log.log("DEBUG", "uid: " + uid + ", nid: " + nid + ", content: " + content + ", typ: " + typ + ", id: " + id + ", topK: " + topK)

    val doc_w = News.calWords(content)

    val doc_vec = News.calVector(doc_w, words_idf)

    val context_vec = CosRecModel.calContextVector(doc_vec, user_vecs, uid)

    if ("news_doc" == typ) {
      CosRecModel.calTopK(context_vec, news_vecs(id), news_titles(id), topK)
    } else {
      CosRecModel.calTopK(context_vec, oversea_news_vecs(id), oversea_news_titles(id), topK)
    }

  }

  def run(host: String, port: Int): Unit = {
    implicit val actorSystem = ActorSystem("BDA")
    val service = actorSystem.actorOf(Props[ServiceActor], "NewsRecommendation")
    IO(Http) ! Http.Bind(service, interface = host, port = port)
  }
}