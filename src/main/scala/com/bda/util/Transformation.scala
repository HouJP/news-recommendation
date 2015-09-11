package com.bda.util

object Transformation {
  /**
   * Reduced by key for an SEQUENCE, and return a MAP.
   * @param seq
   * @param func
   * @tparam K
   * @tparam V
   * @return schema: (key: K, value: V)
   */
  def toMapByKey[K, V](seq: Seq[(K, V)], func: scala.Function2[V, V, V]) : collection.Map[K, V] = {
    val map: collection.mutable.Map[K, V] = collection.mutable.Map[K, V]()
    for (ele <- seq) {
      if (map.contains(ele._1)) {
        map(ele._1) = func(map(ele._1), ele._2)
      } else {
        map(ele._1) = ele._2
      }
    }
    map
  }
}