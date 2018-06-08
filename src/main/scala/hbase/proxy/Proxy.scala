package hbase.proxy

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
    val proxyResponse: Flow[HttpRequest, HttpResponse, Object] = isRequestDoSAttack(request) match {
      case true => Flow[HttpRequest].map(request => HttpResponse(status = StatusCodes.BadRequest))
      case false => Http(system).outgoingConnection(request.uri.authority.host.address(), ClientPort)
    }
    val handler = Source.single(context.request)
      .via(proxyResponse)
      .runWith(Sink.head)
      .flatMap(context.complete(_))
    handler
  }

  val binding = Http(system).bindAndHandle(proxy,Interface, Port)
}

