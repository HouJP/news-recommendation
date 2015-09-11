package com.bda.model

import spray.httpx.Json4sSupport
import org.json4s.Formats
import org.json4s.DefaultFormats
import org.json4s.JsonAST.JObject
import scala.concurrent.duration._
import akka.actor._
import spray.util._
import spray.routing.{HttpService, RequestContext}
import spray.routing.directives.CachingDirectives

class ServiceActor extends Actor with Service {
  implicit def json4sFormats: Formats = DefaultFormats

  def actorRefFactory = context

  def receive = runRoute(queryRoutes)
}

trait Service extends HttpService with Json4sSupport {
  implicit def executionContext = actorRefFactory.dispatcher

  val queryRoutes =
    pathPrefix("golaxy" / Segment) { seg =>
      path("recommend" ) {
        post {
          entity(as[JObject]) { queryObj =>
            complete {
              val typ = seg + "_doc"
              if (Conf.news_doc_type.contains(typ)) {
                val query = queryObj.extract[Query]
                val ans = OnlineProcessor.query(query.uid, query.doc_id, query.content, typ, query.count).map {
                  case (id: String, score: Double) =>
                    Doc(id, score)
                }
                QueryResponse(RecommendResult(0, "Success", ans))
              } else {
                QueryResponse(RecommendResult(1, "Non-standard URL", Seq[Doc]()))
              }
            }
          }
        }
      } ~
      path("update") {
        post {
          entity(as[JObject]) { updateObj =>
            complete {
              val typ = seg + "_doc"
              if (Conf.news_doc_type.contains(typ)) {
                val update = updateObj.extract[Update]
                OnlineProcessor.update(typ, update.filename.charAt(Conf.news_doc_fn_num) - '0')
                UpdateResponse(0, "Success")
              } else {
                UpdateResponse(1, "Non-standard URL")
              }
            }
          }
        }
      }
    } ~
    (post | parameter('method ! "post")) {
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