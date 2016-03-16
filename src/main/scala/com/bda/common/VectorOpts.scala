package com.bda.common

object VectorOpts {

  def calLength(vec: Map[String, Int]): Double = {
    math.sqrt(vec.values.map(e => math.pow(e, 2.0)).sum)
  }
}