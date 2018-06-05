//package hbase.flow
//
//import java.{lang, util}
//
//import akka.http.scaladsl.model.{HttpRequest, HttpResponse, StatusCode}
//import akka.stream._
//import akka.stream.stage._
//import hbase.HBaseConnection
//import org.apache.hadoop.conf.Configuration
//import org.apache.hadoop.hbase.TableName
//import org.apache.hadoop.hbase.client.{Attributes => _, _}
//import org.apache.hadoop.hbase.util.Bytes
//
//import scala.collection.JavaConversions._
//import scala.util.control.NonFatal
//
//class HBF(tableName: TableName, columnFamilies: Set[String], config: Configuration)
//  extends GraphStage[FlowShape[HttpRequest, HttpResponse]] {
//
//  implicit def intToBytes(int: Int) = Bytes.toBytes(int)
//
//  implicit def stringToBytes(string: String) = Bytes.toBytes(string)
//
//  implicit def bytesArrToString(arr: Array[Byte]) = Bytes.toString(arr)
//
//  implicit def bytesArrToInt(arr: Array[Byte]) = Bytes.toInt(arr)
//
//  override protected def initialAttributes: Attributes =
//    Attributes
//      .name("HBaseFLow")
//      .and(
//        ActorAttributes.dispatcher(
//          "akka.stream.default-blocking-io-dispatcher"))
//
//  private val in = Inlet[HttpRequest]("HttpRequest")
//  private val out = Outlet[HttpResponse]("HttpResponse")
//
//  override val shape = FlowShape(in, out)
//
//  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
//    new GraphStageLogic(shape) with StageLogging with HBaseConnection {
//
//      override protected def logSource = classOf[HBF[HttpRequest, HttpResponse]]
//
//      implicit def connection = connect(config)
//
//      val table: Table =
//        getOrCreateTable(tableName, columnFamilies)
//
//      setHandler(out, new OutHandler {
//        override def onPull() = {
//          pull(in)
//        }
//      })
//
//      setHandler(
//        in,
//        new InHandler {
//
//          override def onPush(): Unit = {
//            val request = grab(in)
//            val ipAddressAndPort: String = request.getHeader("Remote-Address").get().value()
//            val ipAddress: String = ipAddressAndPort.split(":")(0)
//            val port: String = ipAddressAndPort.split(":")(1)
//            val get: Get = new Get(ipAddress)
//            columnFamilies.foreach(f => get.addFamily(f))
//            get.setMaxVersions(5)
//            val result: Result = table.get(get)
//
//            val families: util.NavigableMap[Array[Byte], util.NavigableMap[Array[Byte], util.NavigableMap[lang.Long, Array[Byte]]]] = result.getMap
//
//            val versionToValue = collection.mutable.Map[Long, String]()
//
//            for (family <- families.entrySet()) {
//              val familyName: String = family.getKey
//              println(familyName)
//              val columns: util.NavigableMap[Array[Byte], util.NavigableMap[lang.Long, Array[Byte]]] = family.getValue
//              for (column <- columns) {
//                val columnName: String = column._1
//                println(columnName)
//                val timeStampAndValue: util.NavigableMap[lang.Long, Array[Byte]] = column._2
//                for (timeStamp <- timeStampAndValue) {
//                  val version: Long = timeStamp._1
//                  println(version)
//                  val value: String = timeStamp._2
//                  println(value)
//                  versionToValue += (version -> value)
//                }
//              }
//            }
//            push(out,HttpResponse)
//
//          }
//        }
//      )
//
//      override def postStop() = {
//        log.debug("Stage completed")
//        try {
//          table.close()
//          log.debug("table closed")
//        } catch {
//          case NonFatal(ex) =>
//            log.error(ex, "Problem occurred during producer table close")
//        }
//        try {
//          connection.close()
//          log.debug("connection closed")
//        } catch {
//          case NonFatal(ex) =>
//            log.error(ex, "Problem occurred during producer connection close")
//        }
//      }
//    }
//
//}
