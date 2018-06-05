package hbase

import java.{lang, util}

import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes


object M extends App with HBaseConnection {

  implicit def intToBytes(int: Int) = Bytes.toBytes(int)

  implicit def stringToBytes(string: String) = Bytes.toBytes(string)

  implicit def bytesArrToString(arr: Array[Byte]) = Bytes.toString(arr)

  implicit def bytesArrToInt(arr: Array[Byte]) = Bytes.toInt(arr)

  val conf = HBaseConfiguration.create()

  implicit def connection: Connection = connect(conf)

  private val tableName: TableName = TableName.valueOf("proxy", "request")


  private val table: Table = getOrCreateTable(tableName, Set("info"))

  //  private val put = new Put("127.0.0.0")
  //  put.addColumn("info", "port", "123")
  //  table.put(put)


  val get: Get = new Get("127.1.0.0")
  get.setMaxVersions(10)
  get.addColumn("info", "port")

  private val scanner: ResultScanner = table.getScanner(new Scan(get))

  import scala.collection.JavaConversions._


  val result: Result = table.get(get)

  val rowKey: String = result.getRow


  private val families: util.NavigableMap[Array[Byte], util.NavigableMap[Array[Byte], util.NavigableMap[lang.Long, Array[Byte]]]] = result.getMap

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
      }
    }
  }
}





//  scanner.foreach { r =>
//    val rowKey: String = r.getRow
//    val cellScanner: CellScanner = r.cellScanner()
//    r.getValue()
//    cellScanner.advance()
//    val cell = cellScanner.current()
//    val value:String = cell.getValueArray
//    val timestamp = cell.getTimestamp
//  println(s"key : $rowKey value : $value time : $timestamp")
//  }


//  private val cells: util.List[Cell] = result.listCells()
//
//  cells.foreach { cell: Cell =>
//
//    val rowKey: String = cell.getRowArray
//    val family: String = cell.getFamilyArray
//    val column: String = cell.getQualifierArray
//    val timestamp: Long = cell.getTimestamp
//    val value: String = cell.getValueArray
//    println(s"key : $rowKey \nfamily : $family\ncolumn : $column\ntimestamp : $timestamp\nvalue : $value")
//  }
