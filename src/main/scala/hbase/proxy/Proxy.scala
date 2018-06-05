package hbase.proxy

import java.{lang, util}

import akka.NotUsed
import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{HttpRequest, HttpResponse}
import akka.http.scaladsl.server.Route
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Flow, Sink, Source}
import hbase.HBaseConnection
import org.apache.hadoop.hbase.client.{Connection, Get, Result, Table}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}

import scala.collection.JavaConversions._


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


  implicit def intToBytes(int: Int) = Bytes.toBytes(int)

  implicit def stringToBytes(string: String) = Bytes.toBytes(string)

  implicit def bytesArrToString(arr: Array[Byte]) = Bytes.toString(arr)

  implicit def bytesArrToInt(arr: Array[Byte]) = Bytes.toInt(arr)

  val conf = HBaseConfiguration.create()

  implicit def connection: Connection = connect(conf)

  val tableName: TableName = TableName.valueOf("proxy", "request")
  val columnFamilies = Set("info")
  val table: Table = getOrCreateTable(tableName,columnFamilies )


  private val flow: Flow[HttpRequest, HttpResponse, NotUsed] = Flow[HttpRequest].map { request =>
    val ipAddressAndPort: String = request.getHeader("Remote-Address").get().value()
    val ipAddress: String = ipAddressAndPort.split(":")(0)
    val port: String = ipAddressAndPort.split(":")(1)
    val get: Get = new Get(ipAddress)
    columnFamilies.foreach(f => get.addFamily(f))
    get.setMaxVersions(5)
    val result:Result = table.get(get)

    val families: util.NavigableMap[Array[Byte], util.NavigableMap[Array[Byte], util.NavigableMap[lang.Long, Array[Byte]]]] = result.getMap

    val versionToValue = collection.mutable.Map[Long, String]()

    for (family <- families.entrySet()) {
      val familyName: String = family.getKey
      println(familyName)
      val columns: util.NavigableMap[Array[Byte], util.NavigableMap[lang.Long, Array[Byte]]] = family.getValue
      for (column <- columns) {
        val columnName: String = column._1
        println(columnName)
        val timeStampAndValue: util.NavigableMap[lang.Long, Array[Byte]] = column._2
        for (timeStamp <- timeStampAndValue) {
          val version: Long = timeStamp._1
          println(version)
          val value: String = timeStamp._2
          println(value)
          versionToValue += (version -> value)
        }
      }
    }



    if (result==null||versionToValue.size<5){
      HttpResponse()
    }

    val keys:List[Long] = versionToValue.keys.toList

    keys.foreach{
      k=>
        val date = new java.util.Date(k)
        date.
        println(date)
    }


//    val function = NumericRange[Long]

    HttpResponse()
  }

  val outFlow: Flow[HttpResponse, HttpRequest, NotUsed] = {

    null
  }
//  val bidiFlow = BidiFlow.fromFlows(inFlow, outFlow)


}

//val flow: Flow[HttpRequest, HttpResponse, NotUsed] = Flow.fromGraph(hbf)
