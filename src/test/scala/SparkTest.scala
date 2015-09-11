package com.bda.test

import org.apache.spark.sql.{SQLContext, Row}
import org.apache.spark.sql.types.{LongType, StructField, StringType, StructType, IntegerType}
import org.apache.spark.{SparkConf, SparkContext}

import scala.reflect.ClassTag

object SparkTest {
  def main(args: Array[String]) {
    val spark_context = new SparkContext(new SparkConf().setAppName("BDA-NewsRecommendation-OfflineProcessor").setMaster("local"))
    val sql_context = new SQLContext(spark_context)

    val rdd = spark_context.parallelize(
      Seq[(
        String,
        String,
        String,
        String,
        String,
        Long,
        Long,
        String,
        String,
        String,
        String,
        String,
        String,
        Int,
        Int,
        Int,
        Int,
        String)]()).map {
      case ele =>
        Row(
          ele._1,
          ele._2,
          ele._3,
          ele._4,
          ele._5,
          ele._6,
          ele._7,
          ele._8,
          ele._9,
          ele._10,
          ele._11,
          ele._12,
          ele._13,
          ele._14,
          ele._15,
          ele._16,
          ele._17,
          ele._18
        )
    }

    val schema =
      StructType(
        StructField("id", StringType, false) ::
        StructField("t", StringType) ::
        StructField("u", StringType) ::
        StructField("k", StringType) ::
        StructField("a", StringType) ::
        StructField("it", LongType) ::
        StructField("pt", LongType) ::
        StructField("c", StringType) ::
        StructField("au", StringType) ::
        StructField("pl", StringType) ::
        StructField("ol", StringType) ::
        StructField("rl", StringType) ::
        StructField("sn", StringType) ::
        StructField("lng", IntegerType) ::
        StructField("sentiment", IntegerType) ::
        StructField("cmms", IntegerType) ::
        StructField("reps", IntegerType) ::
        StructField("bn", StringType) :: Nil)

  }
}