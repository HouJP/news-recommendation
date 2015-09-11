package com.bda.model

object Conf {
  val docs_dir = "/docs/"
  val news_doc_dir = "/news_doc/"
  val oversea_news_doc_dir = "/oversea_news_doc/"
  val user_info_dir = "/user_info/"

  val cache_dir = "/cache/"
  val docs_num_dir = cache_dir + "/docs_num"
  val words_df_dir = cache_dir + "/words_df"
  val words_idf_dir = cache_dir + "/words_idf"
  val user_vec_dir = cache_dir + "/user_vec"

  val news_doc_fn_pre = "candidate_news_"
  val news_doc_fn_suf = ".txt"
  val news_doc_type = Seq[String]("oversea_news_doc", "news_doc")
  val news_doc_slices_num = 10
  val news_doc_fn_num = 15

  /* parameter */
  val alpha = 0.01
  val user_vector_length = 100
  val user_vector_update = 0.2
  val doc_vector_length = 100
  val jaccard_similarity_thresh = 0.5
  val normalize_penalty = 500
}