package com.houjp.recommendation.service

import com.houjp.recommendation.events.EventsOnlineProcessor
import com.houjp.recommendation.keywords.KeyWordsOnlineProcessor
import spray.httpx.Json4sSupport
import org.json4s.Formats
import org.json4s.DefaultFormats
import org.json4s.JsonAST.JObject
import scala.concurrent.duration._
import akka.actor._
import spray.util._
import spray.routing.HttpService

class ServiceActor extends Actor with Service {
  implicit def json4sFormats: Formats = DefaultFormats

  def actorRefFactory = context

  def receive = runRoute(queryRoutes)
}

trait Service extends HttpService with Json4sSupport {
  implicit def executionContext = actorRefFactory.dispatcher

  val queryRoutes =
    pathPrefix("golaxy" / "recommend") {
      path("key-words") {
        post {
          entity(as[JObject]) { q_obj =>
            complete {
              val query = q_obj.extractOrElse[KeyWordsQuery](null)
              if (null == query) {
                KeyWordsResponse(1, "[ERROR] Post data format error", "")
              } else {
                val rec_ws = KeyWordsOnlineProcessor.query(query.words)
                if (rec_ws.isEmpty) {
                  KeyWordsResponse(1, s"[ERROR] Can not find these words(${query.words})", rec_ws)
                } else {
                  KeyWordsResponse(0, "[SUCCESS]", rec_ws)
                }
              }
            }
          }
        }
      } ~
      path("events") {
        post {
          entity(as[JObject]) { q_obj =>
            complete {
              val query = q_obj.extractOrElse[EventsQuery](null)
              if (null == query) {
                EventsResponse(1, "[ERROR] Post data format error", "")
              } else {
                val rec_evts = EventsOnlineProcessor.query(query.uid)
                if (rec_evts.isEmpty) {
                  KeyWordsResponse(1, s"[ERROR] Can not find the uid(${query.uid})", rec_evts)
                } else {
                  KeyWordsResponse(0, "[SUCCESS]", rec_evts)
                }
              }
            }
          }
        }
      } ~
      path("stop") {
        complete {
          in(1.second){ actorSystem.shutdown() }
          "Shutting down in 1 second..."
        }
      }
    }

  def in[U](duration: FiniteDuration)(body: => U): Unit =
    actorSystem.scheduler.scheduleOnce(duration)(body)

}

case class KeyWordsQuery(words: String)
case class KeyWordsResponse(err_code: Int, err_msg: String, rec_words: String)

case class EventsQuery(uid: String)
case class EventsResponse(err_code: Int, err_msg: String, rec_events: String)