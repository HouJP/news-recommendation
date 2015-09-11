package com.bda.util

import org.ansj.domain.Term
import org.ansj.splitWord.analysis.ToAnalysis

class WordSeg {
  def transform(doc: String): Array[String] = {
    ToAnalysis.parse(doc).toArray.map {
      case t: Term => t.getName
    }.filter(_.size > 1)
  }
}