package com.bda

/**
  * Configuration of examples
  */
package object recommendation {

  /** if is local mode */
  val is_local = false

  /** host and ip */
  val host = "10.100.1.50"
  val port = 8488

  /** data directory */
  val data_pt = "/user/lihb/"

  /** key-words recommendation directory */
  val kw_docs_pt = s"$data_pt/docs/"
  val kw_out_pt = s"$data_pt/keywords_out/"

  /** parameters of key-words recommendation */
  val kw_top_k = 10
  val kw_query_seperator = ","

  /** events recommendation directory */
  val evt_user_pt = s"$data_pt/event_user_info/"
  val evt_docs_pt = s"$data_pt/event_docs/"
  val evt_out_pt = s"$data_pt/event_out/"

  /** parameters of events recommendation */
  val evt_top_k = 10
}
