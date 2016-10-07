package persistent.npm.service.registry

import collection.JavaConverters._
import com.lightbend.lagom.discovery.zookeeper.ZooKeeperServiceRegistry
import java.util.concurrent.TimeUnit
import java.net.URI
import org.apache.curator.x.discovery.UriSpec
import org.apache.curator.x.discovery.ServiceInstance

trait ServiceRegistry {
  private val timeoutInSeconds = 10
  private var zooKeeperServiceRegistry: ZooKeeperServiceRegistry = _

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

  def validateRegisteredService(serviceName: String, serviceId: String,
      serviceHostName: String, servicePort: Int): Boolean = {
    val registeredServices = zooKeeperServiceRegistry.locate(serviceName).toCompletableFuture.get(
        timeoutInSeconds, TimeUnit.SECONDS
      )
    registeredServices.asScala.forall { url => url.getHost.equals(serviceHostName) && url.getPort.equals(servicePort) }
  } 
  
  def registerService(zkUrl: String, zkServicesPath: String,
      serviceName: String, serviceId: String, serviceHostName: String, servicePort: Int): Unit = {
    zooKeeperServiceRegistry = new ZooKeeperServiceRegistry(zkUrl, zkServicesPath);
    zooKeeperServiceRegistry.register(newServiceInstance(serviceName, serviceId, serviceHostName, servicePort))
  }
}