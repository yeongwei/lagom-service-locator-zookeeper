package persistent.npm.service.registry

import collection.JavaConverters._
import com.lightbend.lagom.discovery.zookeeper.ZooKeeperServiceRegistry

import java.util.concurrent.TimeUnit
import java.net.URI

import org.apache.curator.x.discovery.UriSpec
import org.apache.curator.x.discovery.ServiceInstance
import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.retry.RetryForever
import org.apache.curator.x.discovery.ServiceDiscoveryBuilder
import org.apache.curator.x.discovery.ServiceDiscovery
import org.apache.curator.x.discovery.ServiceProvider
import org.apache.curator.framework.recipes.cache.TreeCacheEvent
import org.apache.curator.framework.recipes.cache.TreeCache
import org.apache.curator.framework.recipes.cache.TreeCacheListener
import org.apache.curator.utils.CloseableUtils

import scala.collection.mutable.HashMap
import scala.util.Try
import scala.util.Success
import scala.util.Failure

case class ServiceInfo(serviceInstance: ServiceInstance[String])

trait ServiceAware {
  private val sessionTimeout = 5000
  private val connectionTimeout = 5000
  private val retryInterval = 3000

  private val serviceProviders: HashMap[String, ServiceProvider[String]] = new HashMap[String, ServiceProvider[String]]()

  private var _serviceInstance: ServiceInstance[String] = null /*Service instance of the implementing service*/
  private var _zooKeeperUrl: String = null
  private var _servicesBasePath: String = null

  private var _zooKeeperClient: CuratorFramework = null
  private var _serviceDiscovery: ServiceDiscovery[String] = null
  private var _cache: TreeCache = null

  /**
   * 1. Starts a curatorFramework instance as ZooKeeper client
   * 2. Starts a internal Curator Discovery service instance
   * 3. Starts a TreeCache to monitor the servicesBasePath
   * 4. Resync with any services found under servicesBasePath
   */
  def startServiceAware(zooKeeperUrl: String, sericesBasePath: String): Unit = {
    if (_zooKeeperUrl == null)
      _zooKeeperUrl = zooKeeperUrl

    if (_servicesBasePath == null)
      _servicesBasePath = sericesBasePath

    if (_zooKeeperClient == null) {
      _zooKeeperClient = CuratorFrameworkFactory.newClient(_zooKeeperUrl, sessionTimeout, connectionTimeout, new RetryForever(retryInterval))
      _zooKeeperClient.start()
      _zooKeeperClient.blockUntilConnected()
    }

    if (_serviceDiscovery == null) {
      _serviceDiscovery = ServiceDiscoveryBuilder.builder(classOf[String]).client(_zooKeeperClient).basePath(_servicesBasePath).build()
      _serviceDiscovery.start()
    }

    if (_cache == null) {
      _cache = new TreeCache(_zooKeeperClient, _servicesBasePath)
      _cache.getListenable.addListener(new TreeCacheListener() {
        def childEvent(client: CuratorFramework, event: TreeCacheEvent) =
          cacheEventCallback(client, event)
      })
      _cache.start
    }

    val existingServiceProviders = Try(_zooKeeperClient.getChildren.forPath(_servicesBasePath)) match {
      case Success(paths) => paths.asScala
        .map(path => path.replace("/", ""))
        .map(serviceName => serviceName -> _serviceDiscovery.serviceProviderBuilder().serviceName(serviceName).build()).toMap
      case Failure(f) => new HashMap[String, ServiceProvider[String]]()
    }

    existingServiceProviders.foreach { case (serviceName, serviceProvider) => serviceProvider.start() }
    serviceProviders ++= existingServiceProviders
  }

  def stopServiceAware: Unit = {
    CloseableUtils.closeQuietly(_cache)
    CloseableUtils.closeQuietly(_serviceDiscovery)
    CloseableUtils.closeQuietly(_zooKeeperClient)
    
    _cache = null
    _serviceDiscovery = null
    _zooKeeperClient = null
    _servicesBasePath = null
    _zooKeeperUrl = null
  }

  def registerService(serviceName: String, serviceId: String, serviceHostName: String, servicePort: Int): Unit = {
    _serviceInstance = newServiceInstance(serviceName, serviceId, serviceHostName, servicePort)
    _serviceDiscovery.registerService(_serviceInstance)
  }
  def unregisterService: Unit = _serviceDiscovery.unregisterService(_serviceInstance)
  def discoverService(serviceName: String): ServiceInfo = {
    serviceProviders.get(serviceName) match {
      case Some(serviceProvider) => ServiceInfo(serviceProvider.getInstance)
      case None => ServiceInfo(null)
    }
  }

  private def hasServiceDiscovery: Boolean = if (_serviceDiscovery == null) false else true
  private def hasServiceInstance: Boolean = if (_serviceInstance == null) false else true

  private def newServiceInstance(serviceName: String, serviceId: String,
                                 serviceHostName: String, servicePort: Int): ServiceInstance[String] = {
    ServiceInstance.builder[String]
      .name(serviceName) // e.g. npi-threshold
      .id(serviceId) // e.g. NPI Service UUID ?
      .address(serviceHostName) // e.g. tnpmsmesx0402
      .port(servicePort) // e.g. 9091
      .uriSpec(new UriSpec("{scheme}://{serviceAddress}:{servicePort}")) // e.g. http://tnpmsmesx0402:9091
      .build
  }

  private def getServiceName(path: String): String = path.replace(s"${_servicesBasePath}/", "").split("/").head
  private def handlNodeAdded(event: TreeCacheEvent): Unit = {
    val serviceName = getServiceName(event.getData.getPath)
    if (serviceName.length() > 0) {
      if (!serviceProviders.contains(serviceName)) {
        val serviceProvider = _serviceDiscovery.serviceProviderBuilder().serviceName(serviceName).build()
        serviceProvider.start()
        serviceProviders += serviceName -> serviceProvider
      }
    }
  }
  private def handleNodeRemoved(event: TreeCacheEvent): Unit = serviceProviders.remove(event.getData.getPath)

  private def cacheEventCallback(clent: CuratorFramework, event: TreeCacheEvent): Unit = {
    event.getType match {
      case TreeCacheEvent.Type.NODE_ADDED   => handlNodeAdded(event)
      case TreeCacheEvent.Type.NODE_REMOVED => handleNodeRemoved(event)
      case _                                =>
    }
  }
}