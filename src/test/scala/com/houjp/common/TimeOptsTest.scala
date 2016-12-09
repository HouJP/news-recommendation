package com.houjp.common

import org.scalatest.FunSuite

class TimeOptsTest extends FunSuite {

  test("calDate") {
    val now = "2015-07-23"
    val pre = TimeOpts.calDate(now, -1)

    require("2015-07-22" == pre)
  }
}