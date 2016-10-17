package persistent.npm.service.registry

import akka.actor.ActorLogging
import akka.actor.Actor
import akka.actor.Props

import java.io.File
import java.io.PrintWriter

import org.apache.curator.framework.recipes.cache.TreeCache
import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.retry.RetryForever
import org.apache.curator.framework.recipes.cache.TreeCacheListener
import org.apache.curator.framework.recipes.cache.TreeCacheEvent

import scala.collection.JavaConverters._
import scala.collection.mutable.{ HashMap => MutableHashMap }
import scala.collection.immutable.{ HashMap => ImmutableHashMap }
import scala.collection.immutable.{ Seq => ImmutableSeq }
import scala.util.Try
import scala.util.Failure
import scala.util.Success
import scala.sys.process.stringToProcess

import spray.json.DefaultJsonProtocol
import spray.json.RootJsonFormat
import spray.json.JsValue
import spray.json.JsonParser
import spray.json.JsNumber
import spray.json.JsString
import spray.json.JsObject

protected trait ServiceInstanceJson {
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
  case class ServiceInstance(name: String, id: String, address: String, port: Int, sslPort: String, payload: String,
                             registrationTimeUTC: Long, serviceType: String, uriSpec: UriSpec)
  object ServiceInstanceProtocol extends DefaultJsonProtocol {
    implicit val partFormat = jsonFormat2(Part)
    implicit val uriSpecFormat = jsonFormat1(UriSpec)
    implicit object timeSeriesMetricEntryListFormat extends RootJsonFormat[ServiceInstance] {
      def read(value: JsValue) = {
        def getElseNull(func: () => String): String = Try(func()) match {
          case Success(r) => r
          case Failure(_) => null
        }

        val jsonObject = value.asJsObject

        val name = getElseNull(() => jsonObject.getFields("name").head.asInstanceOf[JsString].value)
        val id = getElseNull(() => jsonObject.getFields("id").head.asInstanceOf[JsString].value)
        val address = getElseNull(() => jsonObject.getFields("address").head.asInstanceOf[JsString].value)
        val port = jsonObject.getFields("port").head.asInstanceOf[JsNumber].value.toInt
        val sslPort = getElseNull(() => jsonObject.getFields("sslPort").head.asInstanceOf[JsString].value) // handle null
        val payload = getElseNull(() => jsonObject.getFields("payload").head.asInstanceOf[JsString].value) // handle null
        val registrationTimeUTC = jsonObject.getFields("registrationTimeUTC").head.asInstanceOf[JsNumber].value.longValue()
        val serviceType = getElseNull(() => jsonObject.getFields("payload").head.asInstanceOf[JsString].value) // handle null
        val uriSpec = jsonObject.getFields("uriSpec").head.convertTo[UriSpec]

        ServiceInstance(name, id, address, port, null, null, registrationTimeUTC, null, uriSpec)
      }
      def write(z: ServiceInstance) = ???
    }

  }
}

protected trait NginxConfiguration {
  def configurationBaseDirectory = "/etc/nginx"
  def confDdirectory = s"${configurationBaseDirectory}/conf.d"
  def defaultDdirectory = s"${configurationBaseDirectory}/default.d"

  def npiServerConf = s"${confDdirectory}/npi-server.conf"
  def npiProxyConf = s"${defaultDdirectory}/npi-proxy.conf"
}

object ServiceRegistry {  
  /**
   * e.g. npi-threshold -> [/services/thresholds/all, /services/threshold/get, ...]
   */
  def serviceMappings: ImmutableHashMap[String, ImmutableSeq[String]] =
    ImmutableHashMap[String, ImmutableSeq[String]](
      "alpha" -> ImmutableSeq("/alpha/api1", "/alpha/api2"),
      "beta" -> ImmutableSeq("/beta/api1", "/beta/api2"),
      "charlie" -> ImmutableSeq("/charlie/api1", "/charlie/api2"))

  def props(zooKeeperUrl: String, servicesBasePath: String) = Props(new ServiceRegistry(zooKeeperUrl, servicesBasePath))
}

class ServiceRegistry(zooKeeperUrl: String, servicesBasePath: String) extends Actor with ActorLogging with ServiceInstanceJson with NginxConfiguration {
  import ServiceInstanceProtocol._

  /**
   * @param state the latest state, either added or updated
   * @param instance the latest data representing a serviceInstance
   */
  private case class ServiceInfo(state: TreeCacheEvent.Type, instance: ServiceInstance)

  private val _sessionTimeout = 5000
  private val _connectionTimeout = 5000
  private val _retryInterval = 5000
  /**
   * e.g. /npi/services/threshold/1 -> NODE_ADDED, ServiceInstance()
   */
  private val _servicesState: MutableHashMap[String, ServiceInfo] = new MutableHashMap[String, ServiceInfo]()

  private var _cache: TreeCache = null
  private var _zooKeeperClient: CuratorFramework = null

  override def preStart = {
    _zooKeeperClient = CuratorFrameworkFactory.newClient(zooKeeperUrl, _sessionTimeout, _connectionTimeout, new RetryForever(_retryInterval))
    _zooKeeperClient.start()
    _zooKeeperClient.blockUntilConnected()
    log.info("ZooKeeper client started.")

    _cache = new TreeCache(_zooKeeperClient, servicesBasePath)
    _cache.getListenable.addListener(new TreeCacheListener() {
      def childEvent(client: CuratorFramework, event: TreeCacheEvent) =
        cacheEventCallback(client, event)
    })
    _cache.start()
    log.info("Curator Tree Cache started.")

    Try(_zooKeeperClient.getChildren.forPath(servicesBasePath)) match {
      case Success(servicePaths) => servicePaths.asScala.foreach { servicePath =>
        Try(_zooKeeperClient.getChildren.forPath(s"${servicesBasePath}/${servicePath}")) match {
          case Success(serviceInstancePaths) => serviceInstancePaths.asScala.foreach { serviceInstancePath =>
            Try(_zooKeeperClient.getData.forPath(s"${servicesBasePath}/${servicePath}/${serviceInstancePath}")) match {
              case Success(data) => {
                val serviceInstance = JsonParser(bytesToString(data)).convertTo[ServiceInstance]
                _servicesState += s"${servicesBasePath}/${servicePath}/${serviceInstancePath}" -> ServiceInfo(TreeCacheEvent.Type.NODE_ADDED, serviceInstance)
              }
              case Failure(f) =>
            }
          }
          case Failure(f) =>
        }
      }
      case Failure(f) =>
    }

    log.info("{} service(s) registered.", _servicesState.size)
  }

  private def bytesToString(bytes: Array[Byte]): String = bytes.map(b => b.toChar).mkString("")
  // Only serviceInstance level has data, e.g. /npi/services/threshold/1
  // e.g. WatchedEvent: TreeCacheEvent{type=NODE_ADDED, data=ChildData{path='/services/service1', stat=5,5,1476429387587,1476429387587,0,1,0,0,0,1,6, data=[]}}
  private def evaluate(event: TreeCacheEvent, evaluateFunc: (TreeCacheEvent) => Unit): Unit = {
    if (event.getData == null)
      log.info("Event {} not evaluated.", event.getType.name())
    else {
      if (event.getData.getData.size > 0)
        evaluateFunc(event)
      else
        log.info("Event {} for path {} not evaluated.", event.getType.name(), event.getData.getPath)
    }
  }

  /**
   * Evaluate based on events, if true then reload service gateway
   */
  private def cacheEventCallback(client: CuratorFramework, event: TreeCacheEvent): Unit = cacheEventCallback(event) match {
    case true  => reloadServiceGateway
    case false => log.info("Do not reload service gateway for {}.", event)
  }

  private def cacheEventCallback(event: TreeCacheEvent): Boolean = event.getType match {
    case TreeCacheEvent.Type.NODE_ADDED   => handleNodeAdded(event)
    case TreeCacheEvent.Type.NODE_REMOVED => handleNodeRemoved(event)
    case TreeCacheEvent.Type.NODE_UPDATED => handleNodeUpdate(event)
    case _ => {
      log.info("Event {} is ignored.", event.getType.name())
      false
    }
  }

  // TODO: Need to revise the scope of this
  /**
   * Generate configurations for NGINX
   * 1. npi-server.conf: serviceName -> serviceHostName + serviceHostPort
   * 2. npi-proxy.conf: url -> protocol + serviceName
   * 3. Directive location might need the = based on explanation at http://nginx.org/en/docs/http/ngx_http_core_module.html#location
   */
  private def reloadServiceGateway: Unit = {
    // e.g. serviceName -> Available [serviceInstance]
    val serviceToInstances = ServiceRegistry.serviceMappings.map {
      case (serviceName, _) => serviceName ->
        _servicesState.filter { case (serviceInstancePath: String, _) => serviceInstancePath.contains(s"/${serviceName}/") }
        .map { case (_, serviceInfo) => serviceInfo }.toSeq
    }

    /*
     * upstream sherpa2 {
				server server1:80; # localhost:99; # server1
        server server2:80; # localhost:88; # server2
				}
     * 
     */
    val servers = new StringBuffer()
    serviceToInstances.filter{ case (_, serviceInstances) => serviceInstances.size > 0 }
      .foreach { case (serviceName, serviceInstances) => {
         servers.append(s"""upstream ${serviceName} {\n""")
         serviceInstances.foreach { case ServiceInfo(_, instance) => servers.append(s"""    server ${instance.address}:${instance.port};\n""") }
         servers.append(s"}\n")
      }
    }
    log.info("npi-server.conf:\n{}", servers.toString())

    /*
     * location /service/api {
				proxy_pass http://sherpa2/service/api/index.html;
				}
     */
    val proxies = new StringBuffer()
    serviceToInstances.filter{ case (_, serviceInstances) => serviceInstances.size > 0 }
      .foreach{ case (serviceName, serviceInstance) => {
        ServiceRegistry.serviceMappings
          .filter{ case (serviceName1, _) => serviceName1.equals(serviceName) }
          .foreach{ case (serviceName2, uris) => uris.foreach { uri => 
            proxies.append(s"""location ${uri} {\n""").append(s"""  proxy_pass http://${serviceName2};\n""").append(s"}\n") }}
    } }
    /*
    ServiceRegistry.serviceMappings.map {
      case (serviceName, uris) => {
        uris.foreach { uri =>
          proxies.append(s"""location ${uri} {\n""").append(s"""  proxy_pass http://${serviceName};\n""").append(s"}\n")
        }
      }
    }
    */
    log.info("npi-proxy.conf:\n{}", proxies.toString())

    def backupIfExist(fullFileName: String): Unit = {
      val file = new File(fullFileName)
      if (file.exists())
        if (file.renameTo(new File(s"${fullFileName}.${System.currentTimeMillis()}")))
          log.info(s"${file.getAbsolutePath} backup created")
        else
          log.info(s"${file.getAbsolutePath} backup not created")
    }

    backupIfExist(npiServerConf)
    backupIfExist(npiProxyConf)

    def writeToFile(fullFileName: String, content: String) = new PrintWriter(fullFileName) {
      write(content)
      close
    }

    writeToFile(npiServerConf, servers.toString())
    writeToFile(npiProxyConf, proxies.toString())

    log.info("About to reload nginx")
    val ret = (s"sudo systemctl reload nginx.service").!!
    log.info(s"${ret}")
  }

  /**
   * e.g. From /npi/services/threshold/1 to threshold
   */
  private def getServiceName(serviceInstancePath: String): String = serviceInstancePath.replace(s"${servicesBasePath}/", "").split("/").head
  private def getRegisteredServices = _servicesState.map{ case (serviceInstancePath, _) => getServiceName(serviceInstancePath) }.toSeq.distinct
  
  /**
   * If found serviceInstance then do nothing else add to map
   * @return true if added
   */
  private def handleNodeAdded(event: TreeCacheEvent): Boolean = {
    val serviceInstancePath = event.getData.getPath
    _servicesState.contains(serviceInstancePath) match {
      case true => {
        log.info("Service instance {} already exist.", serviceInstancePath)
        false
      }
      case false => {
        _servicesState += serviceInstancePath -> ServiceInfo(TreeCacheEvent.Type.NODE_ADDED,
          JsonParser(bytesToString(event.getData.getData)).convertTo[ServiceInstance])
        log.info("Service instance {} added.", serviceInstancePath)
        true
      }
    }
  }

  /**
   * Remove serviceInstance from map with serviceInstancePath
   * @return true if removed
   */
  private def handleNodeRemoved(event: TreeCacheEvent): Boolean = {
    val serviceInstancePath = event.getData.getPath
    _servicesState.remove(serviceInstancePath) match {
      case Some(serviceInfo: ServiceInfo) => {
        log.info("Service instance {} removed.", serviceInstancePath)
        true
      }
      case None => {
        log.info("Service instance {} not found for removal.", serviceInstancePath)
        false
      }
    }
  }

  /**
   * If found serviceInstance then update map else do nothing
   * @return true if updated
   */
  private def handleNodeUpdate(event: TreeCacheEvent): Boolean = {
    val serviceInstancePath = event.getData.getPath
    _servicesState.contains(serviceInstancePath) match {
      case true => {
        _servicesState.update(serviceInstancePath, ServiceInfo(TreeCacheEvent.Type.NODE_UPDATED,
          JsonParser(bytesToString(event.getData.getData)).convertTo[ServiceInstance]))
        true
      }
      case false => {
        log.info("Service instance {} not found for update.", serviceInstancePath)
        false
      }
    }
  }

  def receive = {
    case _@ _ =>
  }
}