package com.bda.common

import java.io.File
import scala.io.Source
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import scala.collection.mutable

object FileOpts {

  /**
    * To detect whether the file exists.
    *
    * @param fp   file path
    * @return     whether the file exists
    */
  def checkFile(fp: String): Boolean = {
    val conf = new Configuration()
    val hdfs = FileSystem.get(conf)
    val pt = new Path(fp)

    hdfs.exists(pt)
  }

  /** Load file from disk.
    *
    * @param fp file path
    * @return   file contents
    */
  def loadFile(fp: String): Seq[String] = {
    Source.fromFile(new File(fp)).getLines().toSeq
  }
}