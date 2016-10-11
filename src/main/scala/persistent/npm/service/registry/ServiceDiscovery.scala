package persistent.npm.service.registry

import collection.JavaConverters._
import com.lightbend.lagom.discovery.zookeeper.ZooKeeperServiceRegistry
import java.util.concurrent.TimeUnit
import java.net.URI
import org.apache.curator.x.discovery.UriSpec
import org.apache.curator.x.discovery.ServiceInstance

trait ServiceDiscovery {
  private val timeoutInSeconds = 10
  private var zooKeeperServiceRegistry: ZooKeeperServiceRegistry = null
  private var serviceInstance: ServiceInstance[String] = null
  private var zooKeeperUrl: String = null
  private var zooKeeperServicesPath: String = null

  private def newServiceInstance(serviceName: String, serviceId: String,
                                 serviceHostName: String, servicePort: Int): ServiceInstance[String] = {
    serviceInstance = ServiceInstance.builder[String]
      .name(serviceName) // e.g. npi-threshold
      .id(serviceId) // e.g. NPI Service UUID ?
      .address(serviceHostName) // e.g. tnpmsmesx0402
      .port(servicePort) // e.g. 9091
      .uriSpec(new UriSpec("{scheme}://{serviceAddress}:{servicePort}")) // e.g. http://tnpmsmesx0402:9091
      .build
    serviceInstance
  }

  private def hasServiceInstance: Boolean = if (serviceInstance == null) false else true
  private def geServiceRegistry(zkUrl: String, zkServicesPath: String) = {
    if (zooKeeperServiceRegistry == null)
      zooKeeperServiceRegistry = new ZooKeeperServiceRegistry(zkUrl, zkServicesPath)
    zooKeeperServiceRegistry
  }
  private def getZooKeeperUrl(_zooKeeperUrl: String): String = {
    if (zooKeeperUrl == null)
      zooKeeperUrl = _zooKeeperUrl
    zooKeeperUrl
  }
  private def getZooKeeperServicesPath(_zooKeeperServicesPath: String): String = {
    if (zooKeeperServicesPath == null)
      zooKeeperServicesPath = _zooKeeperServicesPath
    zooKeeperServicesPath
  }

  def validateRegisteredService: Boolean = if (serviceInstance == null)
      throw new Exception("Service is not registered yet.")
    else   
      validateRegisteredService(serviceInstance.getName, serviceInstance.getId, serviceInstance.getAddress, serviceInstance.getPort)
 
  
  private def validateRegisteredService(serviceName: String, serviceId: String,
                                        serviceHostName: String, servicePort: Int): Boolean = {
    val registeredServices = zooKeeperServiceRegistry.locate(serviceName).toCompletableFuture.get(
      timeoutInSeconds, TimeUnit.SECONDS)
    registeredServices.asScala.forall { url => url.getHost.equals(serviceHostName) && url.getPort.equals(servicePort) }
  }

  /**
   * Handles all object creations needed to facilitate service discovery functionality with order as below,
   * 1. Stores zooKeeperUrl, zkServicesPath
   * 2. Create the zooKeeperRegistry instance
   * 3. Register current Service
   */
  def registerService(zkUrl: String, zkServicesPath: String,
                      serviceName: String, serviceId: String, serviceHostName: String, servicePort: Int): Unit = {
    geServiceRegistry(getZooKeeperUrl(zkUrl), getZooKeeperServicesPath(zkServicesPath))
    zooKeeperServiceRegistry.start()
    zooKeeperServiceRegistry.register(newServiceInstance(serviceName, serviceId, serviceHostName, servicePort))
  }

  def unregisterService: Unit = {
    zooKeeperServiceRegistry.unregister(serviceInstance)
    zooKeeperServiceRegistry.close()
  }
}