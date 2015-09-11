package com.bda.model

import com.bda.util.{Log, Transformation, Operation}
import scala.collection.mutable.{Map => MuMap}

object CosRecModel {

  def calContextVector(doc_vec: Map[String, Double],
                       user_vecs: Map[String, Map[String, Double]],
                       uid: String): Map[String, Double] = {
    val user_vec = user_vecs.getOrElse(uid, Map[String, Double]())

    val context_vec = MuMap[String, Double]()

    user_vec.foreach {
      case (word: String, value: Double) =>
        context_vec(word) = context_vec.getOrElse(word, 0.0) + Conf.alpha * value
    }
    doc_vec.foreach {
      case (word: String, value: Double) =>
        context_vec(word) = context_vec.getOrElse(word, 0.0) + (1 - Conf.alpha) * value
    }

    context_vec.toMap
  }

  def calTopK(context_vec: Map[String, Double],
              news_vecs: Array[(String, Map[String, Double])],
              news_titles: Map[String, String],
              topK: Int): Seq[(String, Double)] = {
    val cos_similarity = news_vecs.map {
      case (id: String, news_vec: Map[String, Double]) =>
        (id, Operation.calCosSimilarity(context_vec, news_vec))
    }.toSeq.sortBy(_._2).reverse

    var ans = Seq[(String, Double)]()

    var index_now = 0
    while (ans.size < topK && index_now < cos_similarity.size) {
      var index_pre = 0
      var flag = true

      for (index_pre <- 0 until index_now) {
        if (Conf.jaccard_similarity_thresh < Operation.calJaccardSimilarity(news_titles.getOrElse(cos_similarity(index_now)._1, ""),
          news_titles.getOrElse(cos_similarity(index_pre)._1, ""))) {
          flag = false
        }
      }

      if (flag) {
        ans = ans :+ (cos_similarity(index_now))
      }

      index_now += 1
    }

    ans
  }
}