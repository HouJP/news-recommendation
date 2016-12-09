package com.houjp.recommendation.news.service

import akka.actor._
import com.houjp.recommendation.news.NewsOnlineProcessor
import org.json4s.JsonAST.JObject
import org.json4s.{DefaultFormats, Formats}
import spray.httpx.Json4sSupport
import spray.routing.HttpService
import spray.util._

import scala.concurrent.duration._

class ServiceActor extends Actor with Service {
  implicit def json4sFormats: Formats = DefaultFormats

  def actorRefFactory = context

  def receive = runRoute(queryRoutes)
}

trait Service extends HttpService with Json4sSupport {
  implicit def executionContext = actorRefFactory.dispatcher

  val queryRoutes =
    pathPrefix("golaxy" / "recommend" / "news") {
      path("query") {
        post {
          entity(as[JObject]) { q_obj =>
            complete {
              val query = q_obj.extractOrElse[NewsQuery](null)
              if (null == query) {
                NewsResponse(1, "[ERROR] Post data format error", "")
              } else {
                val rec_docs = NewsOnlineProcessor.query(query.user_id, query.key_words)
                NewsResponse(0, "[SUCCESS]", rec_docs)
              }
            }
          }
        }
      } ~
        path("stop") {
          complete {
            NewsOnlineProcessor.destruct()
            in(1.second){ actorSystem.shutdown() }
            "Shutting down in 1 second..."
          }
        }
    }

  def in[U](duration: FiniteDuration)(body: => U): Unit =
    actorSystem.scheduler.scheduleOnce(duration)(body)

}

/**
  * Case class of query.
  *
  * @param user_id    user ID
  * @param key_words  key words of news which user is reading
  */
case class NewsQuery(user_id: String, key_words: String)

/**
  * Case class of response.
  *
  * @param err_code   error code
  * @param err_msg    error message
  * @param rec_docs   recommendation documents ID
  */
case class NewsResponse(err_code: Int, err_msg: String, rec_docs: String)
