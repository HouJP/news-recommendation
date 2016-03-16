package com.bda.recommendation.keywords

import com.bda.common.{FileOpts, Log}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}

object IO {

  def loadCoOccurFile(sc: SparkContext, date: String): RDD[((String, String), Int)] = {
    val fp = s"${com.bda.recommendation.kw_out_pt}/$date.obj"

    if (FileOpts.checkFile(fp)) {
      Log.log("INFO", s"Detected $fp")
      sc.objectFile[((String, String), Int)](fp)
    } else {
      Log.log("INFO", s"Do not detected $fp")
      sc.parallelize(Seq[((String, String), Int)]())
    }
  }

  def saveCoOccurFile(co_occur: RDD[((String, String), Int)], date: String): Unit = {
    co_occur.saveAsObjectFile(s"${com.bda.recommendation.kw_out_pt}/$date.obj")
  }

  def loadDocs(sc: SparkContext, sql_c: SQLContext, date: String): DataFrame = {
    val fp = s"${com.bda.recommendation.kw_docs_pt}/$date.txt"

    if (FileOpts.checkFile(fp)) {
      Log.log("INFO", s"Detected $fp")
      sql_c.read.json(fp)
    } else {
      Log.log("INFO", s"Do not detected $fp")
      val docs = sc.parallelize(Seq[(String)]()).map(Row(_))
      val schema = StructType(
        StructField("kv", StringType) :: Nil
      )
      sql_c.createDataFrame(docs, schema)
    }
  }
}