package hbase

import java.{lang, util}

import akka.http.scaladsl.model.HttpRequest
import hbase.proxy.Proxy.{connect, getOrCreateTable}
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}

import scala.collection.JavaConversions._


package object proxy {

  implicit def intToBytes(int: Int) = Bytes.toBytes(int)

  implicit def stringToBytes(string: String) = Bytes.toBytes(string)

  implicit def bytesArrToString(arr: Array[Byte]) = Bytes.toString(arr)

  implicit def bytesArrToInt(arr: Array[Byte]) = Bytes.toInt(arr)

  implicit def booleanToBytes(bool: Boolean) = Bytes.toBytes(bool)

  final val Interface = "localhost"

  final val Port = 9991

  final val ClientPort = 8881

  final val DoSThresholdNumberOfAccess = 5

  final val Versions = 10;

  final val One = 1

  final val FirstElement = 0

  final val SecondElement = 1

  final val OneThousand = 1000

  final val RemoteAddress = "Remote-Address"

  final val ColumnSeparator = ":"

  final val DoSThresholdInSeconds = 10000

  final val HBSchema = "proxy"

  final val HBTable = "request"

  final val ColumnFamilies = Set("info")

  val conf = HBaseConfiguration.create()
  //HDP config
  //conf.set("hbase.zookeeper.property.clientPort", "2181")
  //conf.set("hbase.zookeeper.quorum", "sandbox-hdp.hortonworks.com")
  //conf.set("zookeeper.znode.parent", "/hbase-unsecure")


  implicit def connection: Connection = connect(conf)

  val tableName: TableName = TableName.valueOf(HBSchema, HBTable)
  val table: Table = getOrCreateTable(tableName, ColumnFamilies)

  def isRequestDoSAttack(request: HttpRequest): Boolean = {
    val ipAddressAndPort: String = request.getHeader(RemoteAddress).get().value()
    val ipAddress: String = ipAddressAndPort.split(ColumnSeparator)(FirstElement)
    val port: String = ipAddressAndPort.split(ColumnSeparator)(SecondElement)
    val get: Get = new Get(ipAddress)
    ColumnFamilies.foreach(f => get.addFamily(f))
    get.setMaxVersions(Versions)
    val result: Result = table.get(get)
    result match {
      case res: Result if res.isEmpty => {
        val put = new Put(ipAddress)
        put.addColumn("info", "port", port)
        println(new java.util.Date().getTime)
        table.put(put)
        false
      }
      case res: Result if res.containsColumn("info", "dos") =>
        true
      case res: Result if checkTimeStamps(timeStamps(res)) =>
        val put = new Put(ipAddress)
        put.addColumn("info", "dos", true)
        table.put(put)
        true
      case _ =>
        val put = new Put(ipAddress)
        put.addColumn("info", "port", port)
        println(new java.util.Date().getTime)
        table.put(put)
        false
    }

  }


  def timeStamps(result: Result): List[Long] = {
    val families: util.NavigableMap[Array[Byte], util.NavigableMap[Array[Byte], util.NavigableMap[lang.Long, Array[Byte]]]] = result.getMap
    val versionToValue = collection.mutable.Map[Long, String]()
    for (family <- families.entrySet()) {
      val familyName: String = family.getKey
      val columns: util.NavigableMap[Array[Byte], util.NavigableMap[lang.Long, Array[Byte]]] = family.getValue
      for (column <- columns) {
        val columnName: String = column._1
        val timeStampAndValue: util.NavigableMap[lang.Long, Array[Byte]] = column._2
        for (timeStamp <- timeStampAndValue) {
          val version: Long = timeStamp._1
          val value: String = timeStamp._2
          versionToValue += (version -> value)
        }
      }
    }
    versionToValue.keys.toList.sortWith(_ > _)
  }

  def checkTimeStamps(timeStamps: List[Long]): Boolean = {

    timeStamps match {
      case t if t.size > DoSThresholdNumberOfAccess => t match {
        case t if checkDifferenceBetweenTimestamps(t) <= DoSThresholdInSeconds => {
          true
        }
        case _ => false
      }
      case _ => {
        false
      }

    }

  }

  def checkDifferenceBetweenTimestamps(timeStamps: List[Long]): Long = {
    val timeStampsDescending: List[Long] = timeStamps.take(DoSThresholdNumberOfAccess).sortWith(_ > _)
    val lastTimeStampAccess = timeStampsDescending.head
    val thresholdTimeStamp = timeStampsDescending.drop(DoSThresholdNumberOfAccess - One)(FirstElement)
    val differenceBetweenLastTimeStampAccessAndThresholdTimeStampInSeconds = (lastTimeStampAccess - thresholdTimeStamp) / OneThousand
    differenceBetweenLastTimeStampAccessAndThresholdTimeStampInSeconds
  }

}
