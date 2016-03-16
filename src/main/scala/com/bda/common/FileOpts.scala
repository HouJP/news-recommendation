package com.bda.common

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, FileSystem}

object FileOpts {

  def checkFile(fp: String): Boolean = {
    val conf = new Configuration()
    val hdfs = FileSystem.get(conf)
    val pt = new Path(fp)

    hdfs.exists(pt)
  }
}