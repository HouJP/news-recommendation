package com.bda.util

object OperationTest {
  def testCalVectorLength: Unit = {
    val v1 = Map[String, Double]("a" -> 1.0, "b" -> 2.0, "c" -> 3.0)

    println("test_cal_vector_length: " + Operation.calVectorLength(v1))
  }

  def testCalJaccardSimilarity: Unit = {
    val s1 = "我们 计算"
    val s2 = "计算 科学"

    println("testCalJaccardSimilarity: " + Operation.calJaccardSimilarity(s1, s2))
  }

  def main (args: Array[String]): Unit = {
    testCalVectorLength

    testCalJaccardSimilarity
  }
}