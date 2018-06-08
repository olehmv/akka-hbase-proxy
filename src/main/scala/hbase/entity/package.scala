package hbase

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import org.apache.hadoop.hbase.util.Bytes
import spray.json.DefaultJsonProtocol

package object entity {

  implicit def intToBytes(int: Int) = Bytes.toBytes(int)

  implicit def stringToBytes(string: String) = Bytes.toBytes(string)

  implicit def bytesArrToString(arr: Array[Byte]) = Bytes.toString(arr)

  implicit def bytesArrToInt(arr: Array[Byte]) = Bytes.toInt(arr)

  implicit def booleanToBytes(bool:Boolean)=Bytes.toBytes(bool)

  trait Marshalling extends DefaultJsonProtocol with SprayJsonSupport{

    implicit val personFormat = jsonFormat2(Person)

  }

  case class Person(id:String,name:String)

}
