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
    
    cache = new TreeCache(client, zkServicesPath)    
    cache.getListenable.addListener(listener)
  }

  override def afterAll = {
    system.terminate()
  }

  describe("Service with ServiceDiscover") {
    it("should get event after starting cache") {
      cache.start()
      
      val msg = testProbe.expectMsgClass(new FiniteDuration(10, TimeUnit.SECONDS), classOf[TreeCacheEvent])
      info(s"WatchedEvent: $msg")
      assert(msg.getType == TreeCacheEvent.Type.INITIALIZED)
    }
    
    it("should create the main service path") {
      client.create().forPath(zkServicesPath) // This should be the job of ServiceRegistry ???
      
      val msg = testProbe.expectMsgClass(new FiniteDuration(10, TimeUnit.SECONDS), classOf[TreeCacheEvent])
      info(s"WatchedEvent: $msg")
      assert(msg.getType == TreeCacheEvent.Type.NODE_ADDED)
    }
    
    val serviceName = "service1"
    val serviceId = "1"
      
    it("should register itself upon actor start") {      
      server1 = system.actorOf(MockService.props(zooKeeperServer.getConnectString, zkServicesPath, serviceName, serviceId,
        InetAddress.getLocalHost.getHostName, 9009))

      // serviceName
      val msg = testProbe.expectMsgClass(new FiniteDuration(10, TimeUnit.SECONDS), classOf[TreeCacheEvent])
      info(s"WatchedEvent: $msg")
      assert(msg.getType == TreeCacheEvent.Type.NODE_ADDED)
      info(s"path: ${msg.getData.getPath}")
        
      val allChildren = client.getChildren.forPath(zkServicesPath).asScala
      info(s"allChildren: ${allChildren}")
      val children = allChildren.filter { p => p.equals(serviceName) }
      assert(children.size == 1)
      
      // serviceId
      val msg2 = testProbe.expectMsgClass(new FiniteDuration(10, TimeUnit.SECONDS), classOf[TreeCacheEvent])
      info(s"WatchedEvent: msg2")
      assert(msg.getType == TreeCacheEvent.Type.NODE_ADDED)
      info(s"path: ${msg.getData.getPath}")
      
      val allChildren2 = client.getChildren.forPath(msg.getData.getPath).asScala
      info(s"allChildren2: ${allChildren2}")
      val children2 = allChildren2.filter { p => p.equals(serviceName) }
      assert(children2.size == 0)
      
      val data = client.getData.forPath(msg.getData.getPath)
      info(s"${data.map(b => b.toChar ).mkString("")}")
    }
    
    it("should unregsiter itself actor stop") {
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
}