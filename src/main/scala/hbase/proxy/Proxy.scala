package hbase.proxy

import akka.NotUsed
import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{HttpRequest, HttpResponse, StatusCodes}
import akka.http.scaladsl.server.Route
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Flow, Sink, Source}
import hbase.HBaseConnection
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}

import scala.collection.mutable


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


  val conf = HBaseConfiguration.create()

  implicit def connection: Connection = connect(conf)

  val tableName: TableName = TableName.valueOf("proxy", "request")
  val columnFamilies = Set("info")
  val table: Table = getOrCreateTable(tableName, columnFamilies)


  private val flow: Flow[HttpRequest, HttpResponse, NotUsed] = Flow[HttpRequest].map { request =>
    val ipAddressAndPort: String = request.getHeader("Remote-Address").get().value()
    val ipAddress: String = ipAddressAndPort.split(":")(0)
    val port: String = ipAddressAndPort.split(":")(1)
    val get: Get = new Get("1")
    columnFamilies.foreach(f => get.addFamily(f))
    get.setMaxVersions(5)
    val result: Result = table.get(get)
    result match {
      case res:Result if res.isEmpty=>{
        val put = new Put(ipAddress)
        put.addColumn("info","port",port)
        table.put(put)
        HttpResponse(status = StatusCodes.OK)
      }
      case res:Result if res.containsColumn("info","dos")=>{
        HttpResponse(status=StatusCodes.BadRequest)
      }
      case res:Result if timeStamps(res).size  > 5=>{
        val versionToValue: mutable.Map[Long, String] = timeStamps(res)
        val timeStampsDescending: List[Long] = versionToValue.keys.take(5).toList.sortWith(_ > _)
        val lastTimeStampAccess = timeStampsDescending.head
        val fiveBeforeLastTimeStampAccessTimeStamp = timeStampsDescending.drop(4)(0)
        val differenceBetweenLastTimeStampAccessAndFiveBeforeLastTimeStampAccessTimeStampInSeconds = (lastTimeStampAccess-fiveBeforeLastTimeStampAccessTimeStamp)/ 1000

        HttpResponse()
      }
    }
  }

  val outFlow: Flow[HttpResponse, HttpRequest, NotUsed] = {

    null
  }
  //  val bidiFlow = BidiFlow.fromFlows(inFlow, outFlow)


}

//val flow: Flow[HttpRequest, HttpResponse, NotUsed] = Flow.fromGraph(hbf)

//
//if(result.isEmpty){
//  val put = new Put(ipAddress)
//  HttpResponse()
//}
//
//  val versionToValue:collection.mutable.Map[Long, String] = versions(result)
//
//  if (result == null || versionToValue.size < 5) {
//  HttpResponse()
//}
//
//
//  val keys: List[Long] = versionToValue.keys.toList
//
//  val five = keys.take(5).sortWith(_ > _)
//  five.foreach{
//  l =>
//  println(l)
//  println(new util.Date(l))
//}
//  val first = five.head
//  println(new util.Date(first))
//  val last = five.drop(4)(0)
//  println(new util.Date(last))
//
//  val seconds = (first-last)/ 1000
//
//  if(1<5){
//  val put = new Put(ipAddress)
//  put.addColumn("info","dos",Bytes.toBytes(true))
//  table.put(put)
//  val i=1;
//
//}