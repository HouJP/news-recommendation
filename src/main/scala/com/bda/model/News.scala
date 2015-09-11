package com.bda.model

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import scala.collection.mutable.{Map => MuMap}
import com.bda.util.{Operation, TFIDF}
import com.bda.util.WordSeg

object News extends Serializable {
  def loadDocsNum(sc: SparkContext, data_pt: String): RDD[Int] = {
    sc.objectFile[Int](data_pt)
  }

  def loadWordsDF(sc: SparkContext, data_pt: String): RDD[(String, Int)] = {
    sc.objectFile[(String, Int)](data_pt)
  }

  def loadDocs(spark_context: SparkContext, sql_context: SQLContext, data_pt: String, has_docs: Boolean): DataFrame = {
    if (has_docs) {
      sql_context.read.json(data_pt)
    } else {
      val rdd = spark_context.parallelize(
        Seq[(
            String,
            String,
            String,
            String)]()).map {
        case ele =>
          Row(
            ele._1,
            ele._2,
            ele._3,
            ele._4)
      }

      val schema =
        StructType(
          StructField("id", StringType) ::
          StructField("kv", StringType) ::
          StructField("t", StringType) ::
          StructField("c", StringType) :: Nil)

      sql_context.createDataFrame(rdd, schema)
    }
  }

  def loadWordsIDF(sc: SparkContext, data_pt: String): RDD[(String, Double)] = {
    sc.objectFile[(String, Double)](data_pt)
  }

  def updateDocsNum(pre_docs_num: Int, docs: DataFrame): Int = {
    pre_docs_num + docs.count().toInt
  }

  def getTitle(docs: DataFrame): RDD[(String, String)] = {
    docs.select("id", "t").map {
      case Row(id: String, title: String) =>
        (id, title)
    }
  }

  def updateWordsDF(pre_words_df: RDD[(String, Int)], docs: DataFrame): RDD[(String, Int)] = {
    /* split words */
    val docs_splited = split(docs)
    /* cal docs df */
    val docs_df = TFIDF.calDF(docs_splited)
    /* cal new df */
    (pre_words_df ++ docs_df).reduceByKey((a, b) => a + b)
  }

  def calWordsIDF(words_df: RDD[(String, Int)], docs_num: Int): RDD[(String, Double)] = {
    TFIDF.calIDF(words_df, docs_num)
  }

  def calKeyWords(docs: DataFrame): RDD[(String, Map[String, Int])] = {
    val regex = """\((.+)\)\[([0-9]+)\]""".r
    docs.select("id", "kv").map {
      case Row(id: String, kv: String) =>
        val tfs = MuMap[String, Int]()
        kv.split(";").toSeq.filter {
          (a_kv: String) =>
            try {
              val regex(word, value) = a_kv
              true
            } catch {
              case _ => false
            }
        }.foreach {
          (a_kv: String) =>
            val regex(word, value) = a_kv
            tfs(word) = tfs.getOrElse(word, 0) + value.toInt
        }
        (id, tfs.toMap)
    }
  }

  def calWords(content: String): Map[String, Int] = {
    val tfs = MuMap[String, Int]()
    split(content).foreach {
      case word =>
        tfs(word) = tfs.getOrElse(word, 0) + 1
    }
    tfs.toMap
  }

  def calVector(docs: RDD[(String, Map[String, Int])], words_idf: Map[String, Double]): RDD[(String, Map[String, Double])] = {
    TFIDF.calTFIDFNormalized(docs, words_idf, Conf.normalize_penalty)
  }

  def calVector(doc: Map[String, Int], words_idf: Map[String, Double]): Map[String, Double] = {
    TFIDF.calTFIDFNormalized(doc, words_idf, Conf.normalize_penalty)
  }

  def split(news: DataFrame): RDD[(String, Array[String])] = {
    news.select("id", "t", "c").map {
      case Row(nid: String, title: String, content: String) =>
        (nid, new WordSeg().transform(s"$title $content"))
    }
  }

  def split(news: String): Array[String] = {
    new WordSeg().transform(news)
  }
}