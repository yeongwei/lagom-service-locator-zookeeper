package com.lightbend.lagom.discovery.zookeeper

import akka.actor.ActorSystem
import akka.testkit.TestKit
import akka.testkit.TestProbe

import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

import org.scalatest.BeforeAndAfterAll
import org.scalatest.FunSpec
import org.apache.curator.framework.CuratorFramework
import org.apache.curator.test.TestingServer
import org.apache.curator.x.discovery.ServiceDiscovery
import org.apache.curator.x.discovery.details.ServiceDiscoveryImpl
import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.retry.RetryForever
import org.apache.curator.x.discovery.ServiceDiscoveryBuilder
import org.apache.curator.framework.recipes.cache.TreeCacheListener
import org.apache.curator.framework.recipes.cache.TreeCacheEvent
import org.apache.curator.framework.recipes.cache.TreeCache
import org.apache.curator.x.discovery.ServiceInstance
import org.apache.curator.x.discovery.UriSpec
import org.scalatest.FunSpecLike

import persistent.npm.service.registry.CuratorFrameworkApiSpec
import persistent.npm.service.registry.ServiceDiscoverySpec
import persistent.npm.service.registry.BaseSpec

import scala.concurrent.duration.FiniteDuration

object MultipleZooKeeperServiceDiscoverySpec extends BaseSpec {
  def name = "MultipleZooKeeperServiceDiscoverySpec"
}

class MultipleZooKeeperServiceDiscoverySpec extends TestKit(ActorSystem(MultipleZooKeeperServiceDiscoverySpec.name, MultipleZooKeeperServiceDiscoverySpec.configuration))
    with FunSpecLike with BeforeAndAfterAll {

  private val baseServicePath = "/services"
  private val sessionTimeout = 5000
  private val connectionTimeout = 5000
  private val retryInterval = 3000
  private val counter = new AtomicInteger(0)
  private val testProbe = TestProbe()

  private var zooKeeperServer: TestingServer = _
  private var client1: CuratorFramework = _
  private var client2: CuratorFramework = _
  private var cache: TreeCache = _

  private def listener = new TreeCacheListener() {
    def childEvent(client: CuratorFramework, event: TreeCacheEvent) = {
      info(s"event[${counter.incrementAndGet()}]: ${event}")
      testProbe.send(testProbe.ref, event)
    }
  }

  private def newServiceInstance(serviceName: String, serviceId: String,
                                 serviceHostName: String, servicePort: Int): ServiceInstance[String] = {
    val serviceInstance = ServiceInstance.builder[String]
      .name(serviceName) // e.g. npi-threshold
      .id(serviceId) // e.g. NPI Service UUID ?
      .address(serviceHostName) // e.g. tnpmsmesx0402
      .port(servicePort) // e.g. 9091
      .uriSpec(new UriSpec("{scheme}://{serviceAddress}:{servicePort}")) // e.g. http://tnpmsmesx0402:9091
      .build
    serviceInstance
  }

  override def beforeAll = {
    zooKeeperServer = CuratorFrameworkApiSpec.zooKeeperServer
    zooKeeperServer.start()
  }
  override def afterAll = {
    zooKeeperServer.stop()
    system.terminate()
  }

  var clientForCache: CuratorFramework = _
  var clientForServiceDiscovery: CuratorFramework = _

  describe("Two Service Discovery Instance with same ZooKeeper Client Instance") {
    it("should initialize one ZooKeeper Client for cache") {
      clientForCache = CuratorFrameworkFactory.newClient(zooKeeperServer.getConnectString, sessionTimeout, connectionTimeout, new RetryForever(retryInterval))
      clientForCache.start()
      clientForCache.blockUntilConnected()
      assert(true)
    }
    
    it("should initialize one ZooKeeper Client for Service DIscovery"){
      clientForServiceDiscovery = CuratorFrameworkFactory.newClient(zooKeeperServer.getConnectString, sessionTimeout, connectionTimeout, new RetryForever(retryInterval))
      clientForServiceDiscovery.start()
      clientForServiceDiscovery.blockUntilConnected()
      assert(true)
    }

    it("should start cache") {
      cache = new TreeCache(clientForCache, baseServicePath)
      cache.getListenable.addListener(listener)
      cache.start()

      val msg = testProbe.expectMsgClass(new FiniteDuration(10, TimeUnit.SECONDS), classOf[TreeCacheEvent])
      info(s"WatchedEvent: $msg")
      assert(msg.getType === TreeCacheEvent.Type.INITIALIZED)
      assert(msg.getData === null)
    }

    it("should create 2 instance of Discovery Service and do a couple of things") {
      def createServiceDiscovery(_client: CuratorFramework, _baseServicePath: String) =
        ServiceDiscoveryBuilder.builder(classOf[String]).client(_client).basePath(_baseServicePath).build();
      val serviceDiscovery1 = createServiceDiscovery(clientForServiceDiscovery, baseServicePath)
      val serviceDiscovery2 = createServiceDiscovery(clientForServiceDiscovery, baseServicePath)

      serviceDiscovery1.start()
      serviceDiscovery2.start()

      val service1 = newServiceInstance("serviceA", "1", "hostA", 1011)
      val service2 = newServiceInstance("serviceA", "2", "hostB", 1011)
      
      serviceDiscovery1.registerService(service1)
      serviceDiscovery2.registerService(service2)
      
      serviceDiscovery1.unregisterService(service2) // should have no events at all because not
      
      serviceDiscovery1.unregisterService(service1) // should have events
      serviceDiscovery2.unregisterService(service2) // should have events
      
      assert(true)
    }
    
    it("should get node created for base service path") {
      val msg = testProbe.expectMsgClass(new FiniteDuration(10, TimeUnit.SECONDS), classOf[TreeCacheEvent])
      info(s"WatchedEvent: $msg")
      assert(msg.getType === TreeCacheEvent.Type.NODE_ADDED)
      assert(msg.getData.getPath.equals(baseServicePath))
    }
    
    it("should get node created for service A") {
      val msg2 = testProbe.expectMsgClass(new FiniteDuration(10, TimeUnit.SECONDS), classOf[TreeCacheEvent])
      info(s"WatchedEvent: $msg2")
      assert(msg2.getType === TreeCacheEvent.Type.NODE_ADDED)
      assert(msg2.getData.getPath.equals(s"${baseServicePath}/serviceA"))
    }
    
    it("should get node created for service A id 1") {
      val msg3 = testProbe.expectMsgClass(new FiniteDuration(10, TimeUnit.SECONDS), classOf[TreeCacheEvent])
      info(s"WatchedEvent: $msg3")
      assert(msg3.getType === TreeCacheEvent.Type.NODE_ADDED)
      assert(msg3.getData.getPath.equals(s"${baseServicePath}/serviceA/1"))
    }
    
    it("should get node created for service A id 2") {
      val msg4 = testProbe.expectMsgClass(new FiniteDuration(10, TimeUnit.SECONDS), classOf[TreeCacheEvent])
      info(s"WatchedEvent: $msg4")
      assert(msg4.getType === TreeCacheEvent.Type.NODE_ADDED)
      assert(msg4.getData.getPath.equals(s"${baseServicePath}/serviceA/2"))
    }
    
    it("should get node removed for service A id 1") {
      val msg5 = testProbe.expectMsgClass(new FiniteDuration(10, TimeUnit.SECONDS), classOf[TreeCacheEvent])
      info(s"WatchedEvent: $msg5")
      assert(msg5.getType === TreeCacheEvent.Type.NODE_REMOVED)
      assert(msg5.getData.getPath.equals(s"${baseServicePath}/serviceA/1"))
    }
    
    it("should get node removed for service A id 2") {
      val msg5 = testProbe.expectMsgClass(new FiniteDuration(10, TimeUnit.SECONDS), classOf[TreeCacheEvent])
      info(s"WatchedEvent: $msg5")
      assert(msg5.getType === TreeCacheEvent.Type.NODE_REMOVED)
      assert(msg5.getData.getPath.equals(s"${baseServicePath}/serviceA/2"))
    }
  }
}