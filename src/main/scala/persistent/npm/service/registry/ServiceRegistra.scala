package persistent.npm.service.registry

import com.lightbend.lagom.discovery.zookeeper.ZooKeeperServiceRegistry
import org.apache.curator.x.discovery.ServiceInstance
import org.apache.curator.x.discovery.UriSpec

trait ServiceRegistra {
  private var _serviceInstance: ServiceInstance[String] = null /*Service instance of the implementing service*/
  private var _zooKeeperUrl: String = null
  private var _servicesBasePath: String = null

  private var _zooKeeperServiceRegistry: ZooKeeperServiceRegistry = null

  def startServiceRegistra(zooKeeperUrl: String, sericesBasePath: String): Unit = {
    if (_zooKeeperUrl == null)
      _zooKeeperUrl = zooKeeperUrl

    if (_servicesBasePath == null)
      _servicesBasePath = sericesBasePath

    if (_zooKeeperServiceRegistry == null) {
      _zooKeeperServiceRegistry = new ZooKeeperServiceRegistry(_zooKeeperUrl, _servicesBasePath)
      _zooKeeperServiceRegistry.start()
    }
  }

  def stopServiceRegistra: Unit = {
    _zooKeeperServiceRegistry.close()
    _servicesBasePath = null
    _zooKeeperUrl = null
  }

  def registerService(serviceName: String, serviceId: String, serviceHostName: String, servicePort: Int): Unit =
    if (_serviceInstance == null && _zooKeeperServiceRegistry != null) {
      _serviceInstance = newServiceInstance(serviceName, serviceId, serviceHostName, servicePort)
      _zooKeeperServiceRegistry.register(_serviceInstance)
    }
  def unregisterService: Unit = _zooKeeperServiceRegistry.unregister(_serviceInstance)

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
}