package hbase

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{Admin, Connection, ConnectionFactory, Table}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HColumnDescriptor, HTableDescriptor, NamespaceDescriptor, TableName}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

trait HBaseConnection {

  def connect(conf: Configuration, timeout: Int = 10) =
    Await.result(Future(ConnectionFactory.createConnection(conf)), timeout seconds)

  def getOrCreateTable(tableName: TableName, columnFamilies: Set[String])(implicit connection: Connection): Table = {
    val admin: Admin = connection.getAdmin

    implicit def stringTobytes(s: String) = Bytes.toBytes(s)
    implicit def bytesTostring(arr:Array[Byte])=Bytes.toString(arr)

    if (admin.isTableAvailable(tableName)) {
      connection.getTable(tableName)
    } else {
      val tableDescriptor = new HTableDescriptor(tableName)
//      tableDescriptor.setConfiguration("hbase.table.sanity.checks","false")
      columnFamilies.foreach {
        s =>
          val columnDescriptor = new HColumnDescriptor(s)
          columnDescriptor.setMaxVersions(proxy.Versions)

          //hfile compression algorithm
//          columnDescriptor.setCompressionType(Compression.Algorithm.SNAPPY);
          //hfile block size in kb (default is 64KB)
//          columnDescriptor.setBlocksize(64);
          //enable block cache for every read operation(default is true)
//          columnDescriptor.setBlockCacheEnabled(true)
          // time to life of a value based on the timestamp in seconds
          // (default is Integer.MAX_VALUE treated as live forever)
//          columnDescriptor.setTimeToLive(Integer.MAX_VALUE)

          //keeps all values of colunm family in RegionServer cache
//          columnDescriptor.setInMemory(true)

          // use the row key for the bloom filter(default is BloomType.NONE)
//          columnDescriptor.setBloomFilterType(BloomType.ROW)

          tableDescriptor.addFamily(columnDescriptor)
      }
      val namespace: String = tableName.getNamespace
      val tableExist: Boolean = admin.tableExists(tableName)
      if(!tableExist){
        val buildnameSpace = NamespaceDescriptor.create(namespace).build
//        admin.createNamespace(buildnameSpace)
        admin.createTable(tableDescriptor)
      }
      connection.getTable(tableName)
    }
  }

}
