package com.bda.util

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.rdd._
import scala.collection.mutable.{Map => MuMap}

object TFIDF extends Serializable {
  def calDF(docs_splited: RDD[((String, Array[String]))]): RDD[(String, Int)] = {
    docs_splited.flatMap {
      case (id: String, words: Array[String]) =>
        words.toSet[String].map(word => (word, 1))
    }.reduceByKey((a, b) => a + b)
  }

  def calIDF(words_df: RDD[(String, Int)], docs_num: Int): RDD[(String, Double)] = {
    words_df.map {
      case (word, df) => (word, math.log((docs_num + 1.0) / (df +1.0)))
    }
  }

  /**
   * Caculate IDF value for splited documents.
   * @param docs_splited
   * @return schema: (word: String, value: IDF value).
   */
  def calIDF(docs_splited: RDD[(String, Array[String])]): RDD[(String, Double)] = {
    /* documents num */
    val docs_num = docs_splited.count()

    /* word df */
    val words_df = docs_splited.flatMap {
      case (id: String, words: Array[String]) =>
        words.toSet[String].map(word => (word, 1))
    }.reduceByKey((a, b) => a + b)

    /* words idf */
    words_df.map {
      case (word, df) => (word, math.log((docs_num + 1.0) / (df +1.0)))
    }
  }

  /**
   * Calculate TF-IDF value for documents with words-tf.
   * @param docs_tf schema: RDD[(nid: String, words-tf: Map[word: String, tf: Int])]
   * @param words_idf schema: Map[word: String, idf: Double]
   * @return RDD[(nid: String, tf-idf: Map[word: String, tf-idf: Double])]
   */
  def calTFIDF(docs_tf: RDD[(String, Map[String, Int])],
               words_idf: Map[String, Double]): RDD[(String, Map[String, Double])] = {
    docs_tf.map {
      case (id: String, tfs: Map[String, Int]) =>
        (id, tfs.map {
          case (word: String, tf: Int) =>
            (word, tf * words_idf.getOrElse(word, 0.0))
        })
    }
  }

  def calTFIDF(doc_tf: Map[String, Int], words_idf: Map[String, Double]): Map[String, Double] = {
    doc_tf.map {
      case (word: String, tf: Int) =>
        (word, tf * words_idf.getOrElse(word, 0.0))
    }
  }

  /**
   * Calculate Normalized TF-IDF value for documents with words-tf.
   * @param docs_tf schema: RDD[(nid: String, words-tf: Map[word: String, tf: Int])]
   * @param words_idf schema: Map[word: String, idf: Double]
   * @return RDD[(nid: String, tf-idf-normalized: Map[word: String, value: Double])]
   */
  def calTFIDFNormalized(docs_tf: RDD[(String, Map[String, Int])],
                         words_idf: Map[String, Double],
                         penalty: Int): RDD[(String, Map[String, Double])] = {
    calTFIDF(docs_tf, words_idf).map {
      case (id: String, tf_idf: Map[String, Double]) =>
        val tf_idf_normalized = Operation.normalize(tf_idf, penalty)
        (id, tf_idf_normalized)
    }
  }

  def calTFIDFNormalized(doc_tf: Map[String, Int], words_idf: Map[String, Double], penalty: Int): Map[String, Double] = {
    val tfidf = calTFIDF(doc_tf, words_idf)
    Operation.normalize(tfidf, penalty)
  }
}