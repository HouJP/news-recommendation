package com.houjp.common

object Log {
  def log(typ: String, msg: String): Unit = {
    println("[" + typ + "]\t" + msg)
  }
}