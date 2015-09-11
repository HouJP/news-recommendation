package com.bda.model

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import scala.collection.mutable.{Map => MuMap}
import com.bda.util.Operation

object User {
  def loadUserVec(sc: SparkContext, data_pt: String): RDD[(String, Map[String, Double])] = {
    sc.objectFile[(String, Map[String, Double])](data_pt)
  }

  def loadRec(sc: SparkContext, data_pt: String): RDD[String] = {
    sc.objectFile[String](data_pt)
  }

  def loadHistory(sc: SparkContext, data_pt: String, has_user: Boolean): RDD[(String, String)] = {
    if (has_user) {
      sc.textFile(data_pt).map {
        line =>
          val fields = line.split("\t")
          (fields(0), fields(1))
      }
    } else {
      sc.parallelize(Seq[(String, String)]())
    }
  }

  def calVector(history: RDD[(String, String)],
                docs_vector: RDD[(String, Map[String, Double])]): RDD[(String, Map[String, Double])] = {
    history.map {
      case (uid: String, nid: String) =>
        (nid, uid)
    }.join(docs_vector).map {
      case (nid: String, (uid: String, vector: Map[String, Double])) =>
        (uid, vector)
    }.reduceByKey {
      case (v1: Map[String, Double], v2: Map[String, Double]) =>
        val vector = MuMap[String, Double]()
        v1.foreach {
          case (word: String, value: Double) =>
            vector(word) = vector.getOrElse(word, 0.0) + value
        }
        v2.foreach {
          case (word: String, value: Double) =>
            vector(word) = vector.getOrElse(word, 0.0) + value;
        }
        vector.toMap
    }.map {
      case (uid: String, vector: Map[String, Double]) =>
        val new_vector = vector.toSeq.sortBy(_._2).reverse.take(Conf.user_vector_length).toMap
        (uid, Operation.normalize(new_vector, 0))
    }
  }

  def updateUserVec(pre_user_vec: RDD[(String, Map[String, Double])],
                    user_vec: RDD[(String, Map[String, Double])]): RDD[(String, Map[String, Double])] = {
    (pre_user_vec.map {
      case (uid: String, vec: Map[String, Double]) =>
        (uid, vec.map {
          case (word: String, value: Double) =>
            (word, value * (1 - Conf.user_vector_update))
        })
    } ++ user_vec.map {
      case (uid: String, vec: Map[String, Double]) =>
        (uid, vec.map {
          case (word: String, value: Double) =>
            (word, value * Conf.user_vector_update)
        })
    }).reduceByKey {
      case (v1: Map[String, Double], v2: Map[String, Double]) =>
        val vec = collection.mutable.Map[String, Double]()
        v1.foreach {
          case (word, value) =>
            vec(word) = vec.getOrElse(word, 0.0) + value
        }
        v2.foreach {
          case (word, value) =>
            vec(word) = vec.getOrElse(word, 0.0) + value
        }
        vec.toMap
    }.map {
      case (uid: String, vec: Map[String, Double]) =>
        (uid, Operation.normalize(vec, 0))
    }
  }

  /**
   * Load data as RDD from text-file.
   * @param spark_context
   * @param data_pt
   * @return schema: (uid: String, reading-history: Seq[nid: String])
   */
  def loadText(spark_context: SparkContext, data_pt:String): RDD[(String, String)] = {
    spark_context.textFile(data_pt).map {
      line =>
        val fields = line.split("\t")
        (fields(0), fields(1))
    }
  }

  /**
   * Load object-file of user vector as RDD structure.
   * @param spark_context
   * @param data_pt full path of data file
   * @return (uid: String, vector: Map[word: String, value: Double]).
   */
  def loadObj(spark_context: SparkContext, data_pt: String): RDD[(String, Map[String, Double])] = {
    spark_context.objectFile[(String, Map[String, Double])](data_pt)
  }
}