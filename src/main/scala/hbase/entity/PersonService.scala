package hbase.entity

import akka.NotUsed
import akka.actor.ActorSystem
import akka.event.Logging
import akka.http.scaladsl.Http
import akka.http.scaladsl.Http.ServerBinding
import akka.http.scaladsl.common.{EntityStreamingSupport, JsonEntityStreamingSupport}
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Directives._
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Flow, Keep, Sink, Source}
import hbase.HBaseConnection
import org.apache.hadoop.hbase.client.{Connection, Get, Put, Table}
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

object PersonService extends App {

  implicit val system = ActorSystem("person")
  implicit val materializer = ActorMaterializer()
  implicit val executionContext = system.dispatcher

  val Interface = "localhost"
  val Port = 8881

  private val routs = new PersonService().routs

  val bindingFuture: Future[ServerBinding] =
    Http().bindAndHandle(routs, Interface, Port)

  val log = Logging(system.eventStream, "logs")
  bindingFuture
    .map { serverBinding =>
      log.info(s"Bound to ${serverBinding.localAddress} ")
    }
    .onFailure {
      case ex: Exception =>
        log.error(ex, "Failed to bind to {}:{}!", Interface, Port)
        system.terminate()
    }
}

class PersonService(
                     implicit val executionContext: ExecutionContext,
                     val materializer: ActorMaterializer
                   ) extends Marshalling with HBaseConnection {
  val conf = HBaseConfiguration.create()
  //HDP connection config
  //conf.set("hbase.zookeeper.property.clientPort", "2181")
  //conf.set("hbase.zookeeper.quorum", "sandbox-hdp.hortonworks.com")
  //conf.set("zookeeper.znode.parent", "/hbase-unsecure")

  implicit def connection: Connection = connect(conf)

  final val HBSchema = "users"

  final val HBTable = "person"

  final val ColumnFamilies: Set[String] = Set("info")

  final val ColumnNames: Set[String] = Set("name")

  val tableName: TableName = TableName.valueOf(HBSchema, HBTable)

  val table: Table = getOrCreateTable(tableName, ColumnFamilies)

  def routs = postRoute ~ getRoute

  def postRoute =
    pathPrefix("persons") {
      pathEndOrSingleSlash {
        post {
          entity(as[Person]) { person =>
            val eventualPerson = Source.single(person).via(postFlow).toMat(Sink.head)(Keep.right).run()
            onComplete(
              eventualPerson
            ) {
              case Success(p) => {
                complete(StatusCodes.OK, p)
              }
              case Failure(e) =>
                complete(
                  StatusCodes.BadRequest
                )
            }
          }
        }
      }
    }

  implicit val jsonStreamingSupport: JsonEntityStreamingSupport =
    EntityStreamingSupport
      .json()
      .withParallelMarshalling(parallelism = 8, unordered = false)

  def getRoute =
    pathPrefix("persons" / Segment) { id =>
      pathEndOrSingleSlash {
        get {
          complete {
            val marshallable = Source.single(id).via(getFlow).map[Person](person => {
              val key = person.id
              val value = person.name
              Person(key, value)
            }
            )
            marshallable
          }
        }
      }
    }

  def postFlow: Flow[Person, Person, NotUsed] = Flow[Person].map {
    person =>
      val put = new Put(person.id)
      put.addColumn(ColumnFamilies.head, ColumnNames.head, person.name)
      table.put(put)
      person
  }

  def getFlow: Flow[String, Person, NotUsed] = Flow[String].map {
    str =>
      val get = new Get(str)
      val result = table.get(get)
      val id: String = result.getRow
      val name: String = result.getValue(ColumnFamilies.head, ColumnNames.head)
      Person(id, name)
  }
}
