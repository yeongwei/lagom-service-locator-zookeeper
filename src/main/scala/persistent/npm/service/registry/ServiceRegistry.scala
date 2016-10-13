package persistent.npm.service.registry

import akka.actor.ActorLogging
import akka.actor.Actor
import scala.collection.JavaConverters._
import org.apache.curator.framework.recipes.cache.TreeCache
import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.retry.RetryForever
import org.apache.curator.framework.recipes.cache.TreeCacheListener
import org.apache.curator.framework.recipes.cache.TreeCacheEvent
import scala.collection.mutable.HashMap
import scala.util.Try
import scala.util.Failure
import scala.util.Success
import spray.json.DefaultJsonProtocol
import spray.json.RootJsonFormat
import spray.json.JsValue
import spray.json.JsonParser
import spray.json.JsNumber
import spray.json.JsString
import spray.json.JsObject

protected trait ServiceInstanceJson {
  case class ServiceInfo(state: TreeCacheEvent.Type)
  /*
   * {
   * 	"name":"service1","id":"1","address":"sherpavm","port":9009,"sslPort":null,"payload":null,
   * 	"registrationTimeUTC":1476357809874,"serviceType":"DYNAMIC",
   * 	"uriSpec":{
   * 		"parts":[
   * 			{"value":"scheme","variable":true},
   * 			{"value":"://","variable":false},
   * 			{"value":"serviceAddress","variable":true},
   * 			{"value":":","variable":false},
   * 			{"value":"servicePort","variable":true}]}}
   */
  case class Part(value: String, variable: Boolean)
  case class UriSpec(parts: List[Part])
  case class ServiceInsance(name: String, id: String, address: String, port: Int, sslPort: String, payload: String,
                            registrationTimeUTC: Long, serviceType: String, uriSpec: UriSpec)
  object ServiceInstanceProtocol extends DefaultJsonProtocol {
    implicit val partFormat = jsonFormat2(Part)
    implicit val uriSpecFormat = jsonFormat1(UriSpec)
    implicit object timeSeriesMetricEntryListFormat extends RootJsonFormat[ServiceInsance] {
      def read(value: JsValue) = {
        val jsonObject = value.asJsObject
        val name = jsonObject.getFields("name").head.asInstanceOf[JsString].value
        val id = jsonObject.getFields("id").head.asInstanceOf[JsString].value
        val address = jsonObject.getFields("address").head.asInstanceOf[JsString].value
        val port = jsonObject.getFields("port").head.asInstanceOf[JsNumber].value.toInt
        val sslPort = jsonObject.getFields("sslPort").head // handle null
        val payload = jsonObject.getFields("payload").head // handle null
        val registrationTimeUTC = jsonObject.getFields("registrationTimeUTC").head.asInstanceOf[JsNumber].value.longValue()
        val serviceType = jsonObject.getFields("payload") // handle null
        val uriSpec = jsonObject.getFields("uriSpec").head.convertTo[UriSpec]
        ServiceInsance(name, id, address, port, null, null, registrationTimeUTC, null, uriSpec)
      }
      def write(z: ServiceInsance) = ???
    }

  }
}

class ServiceRegistry(zooKeeperUrl: String, servicesBasePath: String) extends Actor with ActorLogging with ServiceInstanceJson {
  import ServiceInstanceProtocol._

  private val sessionTimeout = 5000
  private val connectionTimeout = 5000
  private val retryInterval = 5000
  private val servicesState: HashMap[String, TreeCacheEvent.Type] = new HashMap[String, TreeCacheEvent.Type]() // e.g. /npi/services/threshold/1 -> NODE_ADDED

  private var _cache: TreeCache = null
  private var _zooKeeperClient: CuratorFramework = null

  override def preStart = {
    _zooKeeperClient = CuratorFrameworkFactory.newClient(zooKeeperUrl, sessionTimeout, connectionTimeout, new RetryForever(retryInterval))
    _zooKeeperClient.start()
    _zooKeeperClient.blockUntilConnected()

    _cache = new TreeCache(_zooKeeperClient, servicesBasePath)
    _cache.getListenable.addListener(new TreeCacheListener() {
      def childEvent(client: CuratorFramework, event: TreeCacheEvent) =
        cacheEventCallback(client, event)
    })
    _cache.start()

    Try(_zooKeeperClient.getChildren.forPath(servicesBasePath)) match {
      case Success(servicePaths) => servicePaths.asScala.foreach { servicePath =>
        Try(_zooKeeperClient.getChildren.forPath(s"${servicesBasePath}/${servicePath}")) match {
          case Success(serviceInstancePaths) => serviceInstancePaths.asScala.foreach { serviceInstancePath =>
            Try(_zooKeeperClient.getData.forPath(s"${servicesBasePath}/${servicePath}/${serviceInstancePath}")) match {
              case Success(data) => JsonParser(data.map(b => b.toChar).mkString("")).convertTo[ServiceInsance]
              case Failure(f)    =>
            }
          }
          case Failure(f) =>
        }
      }
      case Failure(f) =>
    }
  }

  private def evaluate(event: TreeCacheEvent, evaluateFunc: (TreeCacheEvent) => Unit): Unit = {
    if (event.getData != null)
      evaluateFunc(event)
  }
  private def cacheEventCallback(client: CuratorFramework, event: TreeCacheEvent): Unit = event.getType match {
    case TreeCacheEvent.Type.NODE_ADDED   => handlNodeAdded(event)
    case TreeCacheEvent.Type.NODE_REMOVED => handleNodeRemoved(event)
    case TreeCacheEvent.Type.NODE_UPDATED => handleNodeUpdate(event)
    case _                                =>
  }

  // e.g. /npi/services/threshold/1
  private def getServiceName(path: String): String = path.replace(s"${servicesBasePath}/", "").split("/").head
  private def handlNodeAdded(event: TreeCacheEvent): Unit = ???
  private def handleNodeRemoved(event: TreeCacheEvent): Unit = ???
  private def handleNodeUpdate(event: TreeCacheEvent): Unit = ???

  def receive = {
    case _@ _ =>
  }
}