package com.lightbend.lagom.discovery.zookeeper

import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import java.util.Date
import java.text.SimpleDateFormat

import scala.concurrent.duration.FiniteDuration
import scala.collection.mutable.ArrayBuffer

import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.framework.recipes.cache.TreeCache
import org.apache.curator.framework.recipes.cache.TreeCacheEvent
import org.apache.curator.framework.recipes.cache.TreeCacheListener
import org.apache.curator.retry.RetryForever
import org.apache.curator.test.TestingServer
import org.apache.curator.x.discovery.ServiceDiscoveryBuilder
import org.apache.curator.x.discovery.ServiceInstance
import org.apache.curator.x.discovery.UriSpec
import org.apache.zookeeper.data.Stat
import org.scalatest.BeforeAndAfterAll
import org.scalatest.FunSpecLike

import akka.actor.ActorSystem
import akka.testkit.TestKit
import akka.testkit.TestProbe
import persistent.npm.service.registry.BaseSpec
import persistent.npm.service.registry.CuratorFrameworkApiSpec


object MultipleZooKeeperServiceDiscoverySpec extends BaseSpec {
  def name = "MultipleZooKeeperServiceDiscoverySpec"
  def finiteDuration = new FiniteDuration(10, TimeUnit.SECONDS)
}

class MultipleZooKeeperServiceDiscoverySpec extends TestKit(ActorSystem(MultipleZooKeeperServiceDiscoverySpec.name, MultipleZooKeeperServiceDiscoverySpec.configuration))
    with FunSpecLike with BeforeAndAfterAll {

  private val baseServicePath = "/services"
  private val sessionTimeout = 5000
  private val connectionTimeout = 5000
  private val retryInterval = 3000
  private val counter = new AtomicInteger(0)
  private val testProbe = TestProbe()
  private val events = ArrayBuffer[TreeCacheEvent]()

  private var zooKeeperServer: TestingServer = _
  private var client1: CuratorFramework = _
  private var client2: CuratorFramework = _
  private var cache: TreeCache = _

  private def listener = new TreeCacheListener() {
    def childEvent(client: CuratorFramework, event: TreeCacheEvent) = {
      info(s"event[${counter.incrementAndGet()}]: ${event}")
      events.append(event)
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

  private def assertForEvent(eventType: TreeCacheEvent.Type, path: String): TreeCacheEvent = {
    val msg = testProbe.expectMsgClass(MultipleZooKeeperServiceDiscoverySpec.finiteDuration, classOf[TreeCacheEvent])
    info(s"WatchedEvent: $msg")
    assert(msg.getType === eventType)
    if (path == null)
      assert(msg.getData === null)
    else
      assert(msg.getData.getPath.equals(path))
      
    msg
  }

  private def createServiceDiscovery(_client: CuratorFramework, _baseServicePath: String) =
    ServiceDiscoveryBuilder.builder(classOf[String]).client(_client).basePath(_baseServicePath).build();

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

    it("should initialize one ZooKeeper Client for Service DIscovery") {
      clientForServiceDiscovery = CuratorFrameworkFactory.newClient(zooKeeperServer.getConnectString, sessionTimeout, connectionTimeout, new RetryForever(retryInterval))
      clientForServiceDiscovery.start()
      clientForServiceDiscovery.blockUntilConnected()
      assert(true)
    }

    it("should have different session id for both ZooKeeper Client instances") {
      val client1 = clientForCache.getZookeeperClient.getZooKeeper.getSessionId
      val client2 = clientForServiceDiscovery.getZookeeperClient.getZooKeeper.getSessionId
      info(s"client1: ${client1}")
      info(s"client2: ${client2}")
      assert(client1 !== client2)
    }

    it("should start cache") {
      cache = new TreeCache(clientForCache, baseServicePath)
      cache.getListenable.addListener(listener)
      cache.start()

      assertForEvent(TreeCacheEvent.Type.INITIALIZED, null)
    }

    it("should create 2 instance of Discovery Service and perform registration and unregistration") {
      val serviceDiscovery1 = createServiceDiscovery(clientForServiceDiscovery, baseServicePath)
      val serviceDiscovery2 = createServiceDiscovery(clientForServiceDiscovery, baseServicePath)

      serviceDiscovery1.start()
      serviceDiscovery2.start()

      val service1 = newServiceInstance("serviceA", "1", "hostA", 1011)
      val service2 = newServiceInstance("serviceA", "2", "hostB", 1011)

      serviceDiscovery1.registerService(service1) // should have node created events for basePath, servicePath and instancePath
      serviceDiscovery2.registerService(service2) // should have node created event for instancePath

      serviceDiscovery1.unregisterService(service2) // should have no events at all because not

      serviceDiscovery1.unregisterService(service1) // should have node removed event
      serviceDiscovery2.unregisterService(service2) // should have node removed event

      assert(true)
    }

    it(s"should get node created for base service path ${baseServicePath}") {
      assertForEvent(TreeCacheEvent.Type.NODE_ADDED, baseServicePath)
    }

    it("should get node created for service A") {
      assertForEvent(TreeCacheEvent.Type.NODE_ADDED, s"${baseServicePath}/serviceA")
    }

    it("should get node created for service A id 1") {
      assertForEvent(TreeCacheEvent.Type.NODE_ADDED, s"${baseServicePath}/serviceA/1")
    }

    it("should get node created for service A id 2") {
      assertForEvent(TreeCacheEvent.Type.NODE_ADDED, s"${baseServicePath}/serviceA/2")
    }

    it("should get node removed for service A id 1") {
      assertForEvent(TreeCacheEvent.Type.NODE_REMOVED, s"${baseServicePath}/serviceA/1")
    }

    it("should get node removed for service A id 2") {
      assertForEvent(TreeCacheEvent.Type.NODE_REMOVED, s"${baseServicePath}/serviceA/2")
    }
  }

  var clientA: CuratorFramework = _
  var clientB: CuratorFramework = _

  describe("Two Service Discovery Instance with different ZooKeeper Client Instance") {
    it("should initialize two different ZooKeeper Client instances") {
      clientA = CuratorFrameworkFactory.newClient(zooKeeperServer.getConnectString, sessionTimeout, connectionTimeout, new RetryForever(retryInterval))
      clientB = CuratorFrameworkFactory.newClient(zooKeeperServer.getConnectString, sessionTimeout, connectionTimeout, new RetryForever(retryInterval))

      clientA.start()
      clientA.blockUntilConnected()

      clientB.start()
      clientB.blockUntilConnected()

      val clientAid = clientA.getZookeeperClient.getZooKeeper.getSessionId
      val clientBid = clientB.getZookeeperClient.getZooKeeper.getSessionId

      info(s"clientAid: ${clientAid}")
      info(s"clientBid: ${clientBid}")
      assert(clientAid != clientBid)
    }

    it("should create 2 instance of Discovery Service and perform registration and unregistration") {
      val serviceDiscovery1 = createServiceDiscovery(clientA, baseServicePath)
      val serviceDiscovery2 = createServiceDiscovery(clientB, baseServicePath)

      serviceDiscovery1.start()
      serviceDiscovery2.start()

      val service1 = newServiceInstance("serviceB", "1", "host1", 1011)
      val service2 = newServiceInstance("serviceB", "2", "host2", 1011)

      serviceDiscovery1.registerService(service1) // should have node created events for basePath, servicePath and instancePath
      serviceDiscovery2.registerService(service2) // should have node created event for instancePath

      serviceDiscovery1.unregisterService(service1) // should have node removed event
      serviceDiscovery2.unregisterService(service2) // should have node removed event
    }

    it("should get node created for service B") {
      val event = assertForEvent(TreeCacheEvent.Type.NODE_ADDED, s"${baseServicePath}/serviceB")
      val stat = event.getData.getStat
      info(s"When node first added, both creation and modify time should be the same")
      info(s"Ctime: ${stat.getCtime}")
      info(s"Mtime: ${stat.getMtime}")
      assert(stat.getCtime === stat.getMtime)
    }

    it("should get node created for service B id 1") {
      assertForEvent(TreeCacheEvent.Type.NODE_ADDED, s"${baseServicePath}/serviceB/1")
    }

    it("should get node created for service B id 2") {
      assertForEvent(TreeCacheEvent.Type.NODE_ADDED, s"${baseServicePath}/serviceB/2")
    }

    it("should get node removed for service B id 1") {
      assertForEvent(TreeCacheEvent.Type.NODE_REMOVED, s"${baseServicePath}/serviceB/1")
    }

    it("should get node removed for service B id 2") {
      assertForEvent(TreeCacheEvent.Type.NODE_REMOVED, s"${baseServicePath}/serviceB/2")
    }
  }

  describe("Curator Cached Events") {
    it("should buffered events") {
      case class CacheEventRecord(path: String, event: TreeCacheEvent)
      val parsedEvents = events.map { e => {
        if (e.getData == null)
          CacheEventRecord(null, e)
          else 
            CacheEventRecord(e.getData.getPath, e)
      } }
      // path -> [TreeCacheEvent]
      val groupedEventsByPath = parsedEvents.groupBy(r => r.path).map{ case (path, cacheEventRecords) => path -> cacheEventRecords.map { r => r.event }}
      // path -> [TreeCacheEvent] sorted by MTime
      val sortedEventsByPath = groupedEventsByPath.map{ case (path, events) => path -> events.sortBy { e => if (e.getData == null) -1 else e.getData.getStat.getMtime } }
      
      val formatter = new SimpleDateFormat("HH:mm:ss:SSS");
      sortedEventsByPath.foreach { case (path, events) => 
        info(s"${path} ${events.map { e => s"${e.getType.name()}(${if (e.getData == null) -1 else s"Mtime: ${formatter.format(new Date(e.getData.getStat.getMtime))} Mzxid: ${e.getData.getStat.getMzxid}"} )" }.mkString("|")}") }
      assert(true)
    }
  }
}