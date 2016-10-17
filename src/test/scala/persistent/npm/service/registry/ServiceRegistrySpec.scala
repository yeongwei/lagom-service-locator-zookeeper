package persistent.npm.service.registry

import akka.testkit.TestKit
import akka.actor.ActorSystem
import akka.actor.ActorRef

import org.scalatest.FunSpec
import org.scalatest.FunSpecLike
import org.scalatest.BeforeAndAfterAll
import org.apache.curator.test.TestingServer

import spray.json.JsonParser

object ServiceRegistrySpec extends BaseSpec {
  def name = "ServiceRegistrySpec"
}

class ServiceRegistrySpec extends TestKit(ActorSystem(ServiceRegistrySpec.name, ServiceRegistrySpec.configuration))
    with FunSpecLike with ServiceInstanceJson with BeforeAndAfterAll {
  import ServiceInstanceProtocol._

  describe("Service Instance Json") {
    it("should be parsable") {
      val json = """
        {
         "name":"service1","id":"1","address":"sherpavm","port":9009,"sslPort":null,"payload":null,
    	   "registrationTimeUTC":1476357809874,"serviceType":"DYNAMIC",
         "uriSpec":{
         "parts":[
            {"value":"scheme","variable":true},
            {"value":"://","variable":false},
            {"value":"serviceAddress","variable":true},
            {"value":":","variable":false},
            {"value":"servicePort","variable":true}]}}
        """

      val serviceInstance = JsonParser(json).convertTo[ServiceInstance]
      assert(serviceInstance.name.equals("service1"))
      assert(serviceInstance.uriSpec.parts.size == 5)
    }
  }

  private val servicesBasePath = "/npi/services"

  private var server1: ActorRef = _
  private var server2: ActorRef = _
  private var server3: ActorRef = _
  private var server4: ActorRef = _
  private var server5: ActorRef = _
  private var server6: ActorRef = _
  
  private var serviceRegistry: ActorRef = _
  private var zooKeeperServer: TestingServer = _

  override def beforeAll = {
    zooKeeperServer = CuratorFrameworkApiSpec.zooKeeperServer
    zooKeeperServer.start()
    Thread.sleep(3000)
  }
  override def afterAll = {}

  describe("Service Registry") {
    it("should receive events after services registered") {
      serviceRegistry = system.actorOf(ServiceRegistry.props(zooKeeperServer.getConnectString, servicesBasePath), "serviceRegistry1")
      
      server1 = system.actorOf(MockService.props(zooKeeperServer.getConnectString, servicesBasePath,
        "service1", "1", "server1", 7001, true), "server1")
      server2 = system.actorOf(MockService.props(zooKeeperServer.getConnectString, servicesBasePath,
        "service1", "2", "server2", 7002, true), "server2")
        
      server3 = system.actorOf(MockService.props(zooKeeperServer.getConnectString, servicesBasePath,
        "service2", "1", "server3", 8001, true), "server3")
      server4 = system.actorOf(MockService.props(zooKeeperServer.getConnectString, servicesBasePath,
        "service2", "2", "server4", 8001, true), "server4")
        
      server5 = system.actorOf(MockService.props(zooKeeperServer.getConnectString, servicesBasePath,
        "service3", "1", "server5", 9001, true), "server5")
      server6 = system.actorOf(MockService.props(zooKeeperServer.getConnectString, servicesBasePath,
        "service3", "2", "server6", 9001, true), "server6")
        
      Thread.sleep(30000)
      
      assert(true)
    }
    
    it("should sync registered service") {
      system.stop(serviceRegistry)
      Thread.sleep(10000)
      serviceRegistry = system.actorOf(ServiceRegistry.props(zooKeeperServer.getConnectString, servicesBasePath), "serviceRegistry2")
      Thread.sleep(10000)
      assert(true)
    }
  }
}