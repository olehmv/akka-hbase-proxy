package hbase.proxy

import akka.NotUsed
import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{HttpRequest, HttpResponse, StatusCodes}
import akka.http.scaladsl.server.Route
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Flow, Sink, Source}
import hbase.HBaseConnection


object Proxy extends App with HBaseConnection {


  implicit val system = ActorSystem("Proxy")
  implicit val materializer = ActorMaterializer()
  implicit val ec = system.dispatcher

  val proxy = Route { context =>
    val request = context.request
    println("Opening connection to " + request.uri.authority.host.address)


    val handler = Source.single(context.request)
      .via(flow)
      .runWith(Sink.head)
      .flatMap(context.complete(_))
    handler
  }

  val binding = Http(system).bindAndHandle(proxy, "localhost", 8080)

  private val flow: Flow[HttpRequest, HttpResponse, NotUsed] = Flow[HttpRequest].map { request =>
    val bool = isRequestDoSAttack(request)
    bool match {
      case true => HttpResponse(status = StatusCodes.BadRequest)
      case false => HttpResponse(status = StatusCodes.OK)
    }

  }
}
