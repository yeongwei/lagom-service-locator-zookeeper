package persistent.npm.service.registry

import akka.testkit.TestKit
import akka.actor.Actor
import akka.actor.ActorLogging
import akka.actor.ActorRef
import akka.actor.ActorSystem
import akka.actor.Props
import akka.testkit.TestProbe

import com.typesafe.config.ConfigFactory

import java.net.InetAddress
import java.util.concurrent.TimeUnit

import org.scalatest.FunSpecLike
import org.scalatest.BeforeAndAfterAll
import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.zookeeper.Watcher
import org.apache.zookeeper.WatchedEvent
import org.apache.zookeeper.Watcher.Event.KeeperState
import org.apache.zookeeper.ZooKeeper
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.Watcher.Event.EventType
import org.apache.zookeeper.data.Stat

import scala.concurrent.duration.FiniteDuration

object ServiceRegistrySpec {
  def name = "ServiceRegistrySpec"
  def configuration = ConfigFactory.load(
    ConfigFactory.parseString(
      """
        |  akka {
        |    loglevel = "DEBUG"
        |    # log-config-on-start = "on"
        |    remote.netty.tcp.port = 0
        |    remote.netty.tcp.bind-port = 0
        |  }
        """.stripMargin)).withFallback(ConfigFactory.load())
  def zooKeeperHostName = InetAddress.getLocalHost.getHostName
  def zooKeeperPort = 2182
  def zooKeeperUrl = s"${zooKeeperHostName}:${zooKeeperPort}"
}

object ServiceWatcher {
  def props = Props(new ServiceWatcher())
}

class ServiceWatcher extends Actor with ActorLogging {
  def receive = {
    case x @ _ => {
      log.info(s"$x")
      sender ! x
    }
  }
}

class ServiceRegistrySpec
    extends TestKit(ActorSystem(ServiceRegistrySpec.name, ServiceRegistrySpec.configuration))
    with FunSpecLike with BeforeAndAfterAll {

  val testProbe = TestProbe()

  var zooKeeper: ZooKeeper = _
  var serviceWatch: ActorRef = _

  override def beforeAll = {
    serviceWatch = system.actorOf(ServiceWatcher.props)
  }

  override def afterAll = {
    system.terminate()
  }

  private def createWatcher(actor: ActorRef, probe: TestProbe) = new Watcher {
    override def process(we: WatchedEvent) = {
      actor.tell(we, probe.ref)
    }
  }

  describe("A ecosystem with service registry") {
    it("should receive connected event") {
      zooKeeper = new ZooKeeper(ServiceRegistrySpec.zooKeeperUrl, 5000, createWatcher(serviceWatch, testProbe))
      val msg = testProbe.expectMsgClass(new FiniteDuration(10, TimeUnit.SECONDS), classOf[WatchedEvent])
      info(s"WatchedEvent: $msg")
      info(s"SessionId: ${zooKeeper.getSessionId}")
      info(s"SessionTimeout: ${zooKeeper.getSessionTimeout}")
      assert(msg.getState() === KeeperState.SyncConnected)
    }

    val path = "/test"
    it("should not have path created and registers a watcher to the path") {
      val stat = zooKeeper.exists(path, createWatcher(serviceWatch, testProbe))
      info(s"stat: ${stat}")
      assert(stat == null)
    }
    
    val data = "some sample data".toCharArray().map { c => c.toByte }
    it("should create path and receive node create event") {      
      val realPath = zooKeeper.create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL)
      info(s"realPath: ${realPath}")
      assert(realPath.equals(path))

      val msg = testProbe.expectMsgClass(new FiniteDuration(10, TimeUnit.SECONDS), classOf[WatchedEvent])
      info(s"WatchedEvent: $msg")
      assert(msg.getType == EventType.NodeCreated)
    }
    
    it("should change data but no event") {
      val stat = zooKeeper.setData(path, data, -1)
      info(s"stat: ${stat}")
      assert(stat != null)
      testProbe.expectNoMsg()
    }
    
    it("should have path created and registers a watcher") {
      val stat = zooKeeper.exists(path, createWatcher(serviceWatch, testProbe))
      info(s"stat: ${stat}")
      assert(stat != null)
    }

    it("should change data and data changed event produced") {
      val stat = zooKeeper.setData(path, data, -1)
      info(s"stat: ${stat}")
      assert(stat != null)
      val msg = testProbe.expectMsgClass(new FiniteDuration(10, TimeUnit.SECONDS), classOf[WatchedEvent])
      info(s"WatchedEvent: $msg")
      assert(msg.getType == EventType.NodeDataChanged)
    }
  }
}