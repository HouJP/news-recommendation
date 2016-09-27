package com.bda.common

object VectorOpts {

  /**
    * Calculate length of vector.
    *
    * @param vec  vector
    * @return     the length of vector
    */
  def calLength(vec: Map[String, Double]): Double = {
    math.sqrt(vec.values.map(e => math.pow(e, 2.0)).sum)
  }

  /**
    * Calculate length of vector.
    *
    * @param vec  vector
    * @return     the length of vector
    */
  def calLength(vec: Seq[(String, Double)]): Double = {
    math.sqrt(vec.map(_._2).sum)
  }

  /**
    * Calculate dot product of two vectors.
    *
    * @param v1 one of vectors
    * @param v2 another vector
    * @return   dot product of two vectors
    */
  def calDotProduct(v1: Map[String, Double], v2: Seq[(String, Double)]): Double = {
    v2.foldLeft(0.0D) {(s, y) =>
      s + v1.getOrElse(y._1, 0.0D) * y._2
    }
  }

  /**
    * Calculate cosine similarity of two vectors.
    *
    * @param v1 one of vectors
    * @param v2 another vector
    * @return   dot product of two vectors
    */
  def calCosSimilarity(v1: Map[String, Double], v2: Seq[(String, Double)]): Double = {
    val l1 = calLength(v1)
    val l2 = calLength(v2)

    if (l1 < 1e-6 || l2 < 1e-6) {
      0.0D
    } else {
      calDotProduct(v1, v2) / l1 / l2
    }
  }
}