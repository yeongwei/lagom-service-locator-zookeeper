package persistent.npm.service.registry

import akka.actor.ActorLogging
import akka.actor.Actor
import akka.actor.ActorSystem
import akka.testkit.TestKit
import akka.actor.Props
import akka.actor.ActorRef
import akka.testkit.TestProbe

import collection.JavaConverters._

import java.net.InetAddress
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.TimeUnit

import org.scalatest.BeforeAndAfterAll
import org.scalatest.FunSpecLike
import org.apache.curator.test.TestingServer
import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.retry.RetryForever
import org.apache.curator.framework.recipes.cache.TreeCacheListener
import org.apache.curator.framework.recipes.cache.TreeCacheEvent
import org.apache.curator.framework.recipes.cache.TreeCache

import persistent.npm.service.registry.{ ServiceDiscovery => NpiServiceDiscovery }
import scala.concurrent.duration.FiniteDuration

object MockService {
  def props(zkUrl: String, zkServicesPath: String, serviceName: String, serviceId: String, serviceHostName: String, servicePort: Int) =
    Props(new MockService(zkUrl, zkServicesPath, serviceName, serviceId, serviceHostName, servicePort))
}

class MockService(zkUrl: String, zkServicesPath: String, serviceName: String, serviceId: String, serviceHostName: String, servicePort: Int)
    extends Actor with ActorLogging with NpiServiceDiscovery {
  override def preStart = registerService(zkUrl, zkServicesPath, serviceName, serviceId, serviceHostName, servicePort)
  override def postStop = unregisterService

  def receive = {
    case x @ _ => log.info(s"x: ${x}")
  }
}

object ServiceDiscoverySpec extends BaseSpec {
  def name = "ServiceDiscoverySpec"
}

class ServiceDiscoverySpec extends TestKit(ActorSystem(ServiceDiscoverySpec.name, ServiceDiscoverySpec.configuration))
    with FunSpecLike with BeforeAndAfterAll {

  private val sessionTimeout = 5000
  private val connectionTimeout = 5000
  private val retryInterval = 3000
  private val testProbe = TestProbe()
  private val zkServicesPath = "/services"
  private val counter = new AtomicInteger(0)

  private var server1: ActorRef = _
  private var server2: ActorRef = _
  private var zooKeeperServer: TestingServer = _
  private var client: CuratorFramework = _
  private var cache: TreeCache = _

  private def listener = new TreeCacheListener() {
    def childEvent(client: CuratorFramework, event: TreeCacheEvent) = {
      info(s"event[${counter.incrementAndGet()}]: ${event}")
      testProbe.send(testProbe.ref, event)
    }
  }

  override def beforeAll = {
    zooKeeperServer = CuratorFrameworkApiSpec.zooKeeperServer
    zooKeeperServer.start()
    Thread.sleep(3000)

    client = CuratorFrameworkFactory.newClient(zooKeeperServer.getConnectString, sessionTimeout, connectionTimeout, new RetryForever(retryInterval))
    client.start()
  }

  override def afterAll = {
    system.terminate()
  }

  describe("Service Registry emulation") {
    it("should get event after initializing and starting cache") {
      cache = new TreeCache(client, zkServicesPath)
      cache.getListenable.addListener(listener)
      cache.start()

      val msg = testProbe.expectMsgClass(new FiniteDuration(10, TimeUnit.SECONDS), classOf[TreeCacheEvent])
      info(s"WatchedEvent: $msg")
      assert(msg.getType === TreeCacheEvent.Type.INITIALIZED)
      assert(msg.getData === null)
    }
  }

  describe("Services with Service Discovery capabilities") {
    val serviceName = "service1"
    val serviceId = "1"
    val serviceHostName = InetAddress.getLocalHost.getHostName
    val servicePort = 9009

    describe("Service that performs the first registration when zookeeper services path does not even exist") {     
      it("should start the very first service") {
        server1 = system.actorOf(MockService.props(zooKeeperServer.getConnectString, zkServicesPath, serviceName, serviceId,
          serviceHostName, servicePort))
        assert(server1 != null)
      }

      it("should first receive node created event for services path") {
        val msg = testProbe.expectMsgClass(new FiniteDuration(10, TimeUnit.SECONDS), classOf[TreeCacheEvent])
        info(s"WatchedEvent: $msg")
        assert(msg.getType == TreeCacheEvent.Type.NODE_ADDED)
        assert(msg.getData.getPath.equals(zkServicesPath))
      }

      it("should followed by node created event for service name") {
        val msg = testProbe.expectMsgClass(new FiniteDuration(10, TimeUnit.SECONDS), classOf[TreeCacheEvent])
        info(s"WatchedEvent: $msg")
        assert(msg.getType == TreeCacheEvent.Type.NODE_ADDED)
        assert(msg.getData.getPath.equals(s"${zkServicesPath}/${serviceName}"))
      }

      it("should followed by node created event for service id") {
        val msg = testProbe.expectMsgClass(new FiniteDuration(10, TimeUnit.SECONDS), classOf[TreeCacheEvent])
        info(s"WatchedEvent: $msg")
        assert(msg.getType == TreeCacheEvent.Type.NODE_ADDED)
        assert(msg.getData.getPath.equals(s"${zkServicesPath}/${serviceName}/${serviceId}"))
      }

      it("should get data at service id level") {
        val data = client.getData.forPath(s"${zkServicesPath}/${serviceName}/${serviceId}")
        val dataStr = { data.map(b => b.toChar).mkString("") }
        info(s"dataStr: ${dataStr}")
        assert(dataStr.contains(serviceHostName))
        assert(dataStr.contains(String.valueOf(servicePort)))
      }

      it("should unregsiter itself upon actor stop") {
        system.stop(server1)
        val msg = testProbe.expectMsgClass(new FiniteDuration(10, TimeUnit.SECONDS), classOf[TreeCacheEvent])
        info(s"WatchedEvent: $msg")
        assert(msg.getType == TreeCacheEvent.Type.NODE_REMOVED)

        val allChildren = client.getChildren.forPath(s"${zkServicesPath}/${serviceName}").asScala
        info(s"allChildren: ${allChildren}")
        val children = allChildren.filter { p => p.equals(serviceName) }
        assert(children.size == 0)
      }
    }

    val serviceId2 = "2"
    val servicePort2 = 9011
    describe("Multiple services with Service discovery capabilities") {
      it("should have no children under service name") {
        assert(client.getChildren.forPath(s"${zkServicesPath}/${serviceName}").asScala.size == 0)
      }
            
      it("should start service 1") {
        server1 = system.actorOf(MockService.props(zooKeeperServer.getConnectString, zkServicesPath, serviceName, serviceId,
          serviceHostName, servicePort))
        assert(server1 != null)
      }
      
      it("should receive node created event for server1 for service id level") {
        val msg = testProbe.expectMsgClass(new FiniteDuration(10, TimeUnit.SECONDS), classOf[TreeCacheEvent])
        info(s"WatchedEvent: $msg")
        assert(msg.getType == TreeCacheEvent.Type.NODE_ADDED)
        assert(msg.getData.getPath.equals(s"${zkServicesPath}/${serviceName}/${serviceId}"))
      }
      
      it("should start service 2") {
        server2 = system.actorOf(MockService.props(zooKeeperServer.getConnectString, zkServicesPath, serviceName, serviceId2,
          serviceHostName, servicePort2))
        assert(server2 != null)
      }
      
      ignore("should receive node created event for server1 for service id 2 level") {
        val msg = testProbe.expectMsgClass(new FiniteDuration(10, TimeUnit.SECONDS), classOf[TreeCacheEvent])
        info(s"WatchedEvent: $msg")
        assert(msg.getType == TreeCacheEvent.Type.NODE_ADDED)
        assert(msg.getData.getPath.equals(s"${zkServicesPath}/${serviceName}/${serviceId2}"))
      }
      
      ignore("should store independent data on server id level") {
        Thread.sleep(3000)
        val data = client.getData.forPath(s"${zkServicesPath}/${serviceName}/${serviceId}")
        val dataStr = { data.map(b => b.toChar).mkString("") }
        info(s"dataStr: ${dataStr}")
        assert(dataStr.contains(serviceHostName))
        assert(dataStr.contains(String.valueOf(servicePort)))
        
//        val data2 = client.getData.forPath(s"${zkServicesPath}/${serviceName}/${serviceId2}")
//        val dataStr2 = { data2.map(b => b.toChar).mkString("") }
//        info(s"dataStr: ${dataStr2}")
//        assert(dataStr2.contains(serviceHostName))
//        assert(dataStr2.contains(String.valueOf(servicePort2)))
      }
    }
  }
}