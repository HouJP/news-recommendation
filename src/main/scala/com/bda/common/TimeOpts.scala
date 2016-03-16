package com.bda.common

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

object TimeOpts {

  def calDate(date_s: String, d_bias: Int): String = {
    val f = new SimpleDateFormat("yyyy-MM-dd")

    val c = Calendar.getInstance()
    val date = f.parse(date_s)
    c.setTime(date)

    val d = c.get(Calendar.DATE)
    c.set(Calendar.DATE, d + d_bias)

    f.format(c.getTime)
  }
}