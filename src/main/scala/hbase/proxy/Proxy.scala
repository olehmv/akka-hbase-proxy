package hbase.proxy

import akka.NotUsed
import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{HttpRequest, HttpResponse, StatusCodes}
import akka.http.scaladsl.server.Route
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Flow, Sink, Source}
import hbase.HBaseConnection
import hbase.entity.PersonService

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

object Proxy extends App with HBaseConnection {


  implicit val system = ActorSystem("Proxy")
  implicit val materializer = ActorMaterializer()
  implicit val ec = system.dispatcher

  private val routs = new PersonService().routs



  val proxy = Route { context =>
    println("request time: "+new java.util.Date().getTime)
    val request = context.request
    val flow: Flow[HttpRequest, HttpResponse, NotUsed] = Flow[HttpRequest].map { request =>
      val bool = isRequestDoSAttack(request)
      bool match {
        case true => HttpResponse(status = StatusCodes.BadRequest)
        case false =>

          val source = Source.single(request).via(routs)
          val eventualResponse: Future[HttpResponse] = source.runWith(Sink.head)
          val response: HttpResponse = Await.result(eventualResponse,1 seconds)
          response
      }
    }
    println("Opening connection to " + request.uri.authority.host.address)
    val handler = Source.single(request)
      .via(flow)
      .runWith(Sink.head)
      .flatMap(context.complete(_))
    handler
  }

  val binding = Http(system).bindAndHandle(proxy, "localhost", 9999)

}
