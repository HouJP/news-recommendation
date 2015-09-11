package com.bda.util

import com.bda.model.Conf

object Operation {
  /**
   * Calculate vector's length.
   * @param vector
   * @return length of the vector.
   */
  def calVectorLength(vector: Map[String, Double]): Double = {
    var ret = 0.0
    for (tuple <- vector) {
      ret += tuple._2 * tuple._2
    }
    math.sqrt(ret)
  }

  /**
   * Calculate cosine similarity between two vectors.
   * @param v1 schema: (word: String, value: Double)
   * @param v2 schema: (word: String, value: Double)
   * @return cosine similarity.
   */
  def calCosSimilarity(v1: Map[String, Double],
                       v2: Map[String, Double]): Double = {
    val v1_length = calVectorLength(v1)
    val v2_length = calVectorLength(v2)
    var ret = 0.0
    for (tuple <- v1) {
      ret += tuple._2 * v2.getOrElse(tuple._1, 0.0)
    }

    if (0.0 >= v1_length || 0.0 >= v2_length) {
      0.0
    } else {
      ret / v1_length / v2_length
    }
  }

  /**
   * Nomalize vector.
   * @param v schema: Map[word: String, value: Double]
   * @return schema: Map[word: String, value: Double]
   */
  def normalize(v: Map[String, Double], penalty: Int): Map[String, Double] = {
    val length = calVectorLength(v)
    if (0.0 >= length) {
      v
    } else {
      v.map {
        case (word: String, value: Double) =>
          (word, value / (length + penalty))
      }
    }
  }

  /**
   * Calculate JaccardSimilarity for two strings.
   * @param s1
   * @param s2
   * @return jaccard similarity
   */
  def calJaccardSimilarity(s1: String, s2: String): Double = {
    val w1 = new WordSeg().transform(s1).toSet
    val w2 = new WordSeg().transform(s2).toSet

    val w = w1 ++ w2

    val union_size = w.size.toDouble
    val intersect_size = (w1.size + w2.size).toDouble - w.size.toDouble

    if (0 >= union_size) {
      0.0
    } else {
      intersect_size / union_size
    }
  }
}