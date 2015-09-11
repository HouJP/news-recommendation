package com.bda.model

case class Query(uid: String, doc_id: String, content: String, count: Int) {}
case class Doc(id: String, score: Double)
case class RecommendResult(error_code: Int, error_message: String, docs: Seq[Doc])
case class QueryResponse(recommend_result: RecommendResult) {}

case class Update(dir: String, filename: String) {}
case class UpdateResponse(error_code: Int, error_message: String)