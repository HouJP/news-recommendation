package com.bda.util

object Log {
  def log(typ: String, msg: String): Unit = {
    println("[" + typ + "]\t" + msg)
  }
}