package persistent.npm.service.registry

import java.net.InetAddress
import java.text.SimpleDateFormat
import java.util.Date
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.JavaConverters.asScalaBufferConverter
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration.FiniteDuration

import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.framework.recipes.cache.TreeCache
import org.apache.curator.framework.recipes.cache.TreeCacheEvent
import org.apache.curator.framework.recipes.cache.TreeCacheListener
import org.apache.curator.retry.RetryForever
import org.apache.curator.test.TestingServer
import org.scalatest.BeforeAndAfterAll
import org.scalatest.FunSpecLike

import akka.actor.Actor
import akka.actor.ActorLogging
import akka.actor.ActorRef
import akka.actor.ActorSystem
import akka.actor.Props
import akka.actor.actorRef2Scala
import akka.testkit.TestKit
import akka.testkit.TestProbe

object MockService {
  def props(zkUrl: String, zkServicesPath: String, serviceName: String, serviceId: String, serviceHostName: String, servicePort: Int, registerService: Boolean) =
    Props(new MockService(zkUrl, zkServicesPath, serviceName, serviceId, serviceHostName, servicePort, registerService))
}

class MockService(zkUrl: String, zkServicesPath: String, serviceName: String, serviceId: String, serviceHostName: String, servicePort: Int, registerServiceRequired: Boolean)
    extends Actor with ActorLogging with ServiceRegistrar {

  override def preStart = {
    startServiceRegistrar(zkUrl, zkServicesPath)
    if (registerServiceRequired)
      registerService(serviceName, serviceId, serviceHostName, servicePort)
      
    log.info(s"zkUrl: ${zkUrl} zkServicesPath: ${zkServicesPath} serviceName: ${serviceName} serviceId: ${serviceId} serviceHostName: ${serviceHostName} servicePort: ${servicePort} registerServiceRequired: ${registerServiceRequired}")
  }
  override def postStop = {
    unregisterService
    stopServiceRegistrar
  }

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
  private val events = ArrayBuffer[TreeCacheEvent]()

  private var server1: ActorRef = _
  private var server2: ActorRef = _
  private var server3: ActorRef = _
  private var zooKeeperServer: TestingServer = _
  private var client: CuratorFramework = _
  private var cache: TreeCache = _

  private case class CacheEventRecord(path: String, event: TreeCacheEvent)

  private def listener = new TreeCacheListener() {
    def childEvent(client: CuratorFramework, event: TreeCacheEvent) = {
      // info(s"event[${counter.incrementAndGet()}]: ${event}")
      events.append(event)
      testProbe.send(testProbe.ref, event)
    }
  }

  private def assertForEvent(eventType: TreeCacheEvent.Type, path: String): TreeCacheEvent = {
    val msg = testProbe.expectMsgClass(new FiniteDuration(10, TimeUnit.SECONDS), classOf[TreeCacheEvent])
    info(s"WatchedEvent: $msg")
    assert(msg.getType === eventType)
    if (path == null)
      assert(msg.getData === null)
    else
      assert(msg.getData.getPath.equals(path))

    msg
  }

  private def getEvents = events.map { e =>
    {
      if (e.getData == null)
        CacheEventRecord(null, e)
      else
        CacheEventRecord(e.getData.getPath, e)
    }
  }

  private def getEventsGroupedByPath =
    getEvents.groupBy(rec => rec.path).map { case (path, cacheEventRecords) => path -> cacheEventRecords.map { r => r.event } }

  private def getSortedEventsGroupByPath =
    getEventsGroupedByPath.map { case (path, events) => path -> events.sortBy { e => if (e.getData == null) -1 else e.getData.getStat.getMtime } }

  private def printEvents(allEvents: Map[String, ArrayBuffer[TreeCacheEvent]]) = {
    val formatter = new SimpleDateFormat("HH:mm:ss:SSS")
    allEvents.foreach {
      case (path, events) =>
        info(s"${path} ${events.map { e => s"${e.getType.name()}(${if (e.getData == null) -1 else s"Mtime: ${formatter.format(new Date(e.getData.getStat.getMtime))} Czxid: ${e.getData.getStat.getCzxid} Mzxid: ${e.getData.getStat.getMzxid}"} )" }.mkString("|")}")
    }
  }

  private def getEventualEvents(allEvents: Map[String, ArrayBuffer[TreeCacheEvent]]) =
    allEvents.filter { case (path, events) => path != null }.map { case (path, events) => path -> events.sortBy { e => e.getData.getStat.getMtime }.reverse.head }

  override def beforeAll = {
    zooKeeperServer = CuratorFrameworkApiSpec.zooKeeperServer
    zooKeeperServer.start()
    Thread.sleep(3000)

    client = CuratorFrameworkFactory.newClient(zooKeeperServer.getConnectString, sessionTimeout, connectionTimeout, new RetryForever(retryInterval))
    client.start()
  }

  override def afterAll = {
    system.terminate()

    getEvents.foreach { rec => info(s"${rec.path} ${rec.event.getType.name()}") }
  }

  ignore("A fresh ZooKeeper instance") {
    it("should still behave even if path is not created yet") {
      try {
        client.getChildren.forPath(zkServicesPath)
      } catch {
        case ex: Throwable => {
          ex.printStackTrace()
          assert(false)
        }
      }
    }
  }
  
  describe("Service Registry emulation") {
    it("should get event after initializing and starting cache") {
      cache = new TreeCache(client, zkServicesPath)
      cache.getListenable.addListener(listener)
      cache.start()
      assertForEvent(TreeCacheEvent.Type.INITIALIZED, null)
    }
  }

  describe("Services with Service Aware capabilities") {
    val serviceName = "service1"
    val serviceId = "1"
    val serviceHostName = InetAddress.getLocalHost.getHostName
    val servicePort = 9009

    describe("Service that performs the first registration when zookeeper services path does not even exist") {
      it("should start the very first service") {
        server1 = system.actorOf(MockService.props(zooKeeperServer.getConnectString, zkServicesPath, serviceName, serviceId,
          serviceHostName, servicePort, true))
        assert(server1 != null)
      }

      it("should first receive node created event for services path") {
        assertForEvent(TreeCacheEvent.Type.NODE_ADDED, zkServicesPath)
      }

      it("should followed by node created event for service name") {
        assertForEvent(TreeCacheEvent.Type.NODE_ADDED, s"${zkServicesPath}/${serviceName}")
      }

      it("should followed by node created event for service id") {
        assertForEvent(TreeCacheEvent.Type.NODE_ADDED, s"${zkServicesPath}/${serviceName}/${serviceId}")
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
        assertForEvent(TreeCacheEvent.Type.NODE_REMOVED, s"${zkServicesPath}/${serviceName}/${serviceId}")

        val allChildren = client.getChildren.forPath(s"${zkServicesPath}/${serviceName}").asScala
        info(s"allChildren: ${allChildren}")
        val children = allChildren.filter { p => p.equals(serviceName) }
        assert(children.size == 0)
      }
    }

    val serviceId2 = "2"
    val servicePort2 = 9011
    describe("Multiple services with Service Aware capabilities") {
      it("should have no children under service name") {
        client.getChildren.forPath(s"${zkServicesPath}").asScala.foreach { p => info(p) }
        assert(client.getChildren.forPath(s"${zkServicesPath}/${serviceName}").asScala.size == 0)
      }

      it("should start service 1") {
        server1 = system.actorOf(MockService.props(zooKeeperServer.getConnectString, zkServicesPath, serviceName, serviceId,
          serviceHostName, servicePort, true), "server1")
        assert(server1 != null)
      }

      it("should start service 2") {
        server2 = system.actorOf(MockService.props(zooKeeperServer.getConnectString, zkServicesPath, serviceName, serviceId2,
          serviceHostName, servicePort2, true), "server2")
        assert(server2 != null)
      }

      it("should evaluate the eventual states of service instance path") {
        Thread.sleep(5000)
        val eventualEvents = getEventualEvents(getSortedEventsGroupByPath)
        assert(eventualEvents != null)
        eventualEvents.foreach { case (path, event) => info(s"${path}") }

        assert(eventualEvents.contains(s"${zkServicesPath}/${serviceName}/${serviceId}"))
        val result = eventualEvents.get(s"${zkServicesPath}/${serviceName}/${serviceId}").get
        assert(result.getType.equals(TreeCacheEvent.Type.NODE_ADDED))

        assert(eventualEvents.contains(s"${zkServicesPath}/${serviceName}/${serviceId2}"))
        val result2 = eventualEvents.get(s"${zkServicesPath}/${serviceName}/${serviceId2}").get
        assert(result2.getType.equals(TreeCacheEvent.Type.NODE_ADDED))
      }
      
      it("should have 2 children as services") {
        val allServices = client.getChildren.forPath(s"${zkServicesPath}").asScala
        info(s"allServices: ${allServices}")
        assert(allServices.size > 0)
      }
    }
    
    describe("Access to service instance data") {
      it("should be parsable") {
        val data = client.getData.forPath(s"${zkServicesPath}/${serviceName}/${serviceId}")
        val dataStr = { data.map(b => b.toChar).mkString("") }
        info(s"dataStr: ${dataStr}")
      }
    }
  }
}