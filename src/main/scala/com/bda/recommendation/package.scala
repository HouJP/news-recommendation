package com.bda

/**
  * Configuration of examples
  */
package object recommendation {

  /** if is local mode */
  val is_local = false

  /** host and ip */
  val host = "10.1.111.15"
//  val host = "127.0.0.1"
  val port = 8488

  /** data directory */
//  val data_pt = "/Users/houjianpeng/Github/news-recommendation/data/" // local path
  val data_pt = "/home/recommendation/data/" // server path

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

  /** news recommendation directory */
  val news_user_pt = s"$data_pt/news_user/"
  val news_doc_pt = s"$data_pt/news_doc/"

  /** parameters of news recommendation */
  val news_top_k = 10
}
