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
import java.net.ServerSocket
import java.util.concurrent.TimeUnit

import org.apache.curator.test.TestingServer
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
import org.scalatest.FunSpecLike
import org.scalatest.BeforeAndAfterAll

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

  def localZooKeeperHostName = InetAddress.getLocalHost.getHostName
  def localZooKeeperPort = 2182
  def zooKeeperUrl = s"${localZooKeeperHostName}:${localZooKeeperPort}"

  def getRandomPort = new ServerSocket(0).getLocalPort
}

class ServiceRegistrySpec
    extends TestKit(ActorSystem(ServiceRegistrySpec.name, ServiceRegistrySpec.configuration))
    with FunSpecLike with BeforeAndAfterAll {

  private val testProbe = TestProbe()
  private val zooKeeperClientTimeout = 5000
  
  private var server: TestingServer = _
  private var zooKeeper: ZooKeeper = _
  private var startTimestamp: Long = _

  override def beforeAll = {
    startTimestamp = System.currentTimeMillis()
    server = new TestingServer(-1)
    server.start
    Thread.sleep(3000)
    info(s"zookeeperUrl: ${server.getConnectString}")
  }

  override def afterAll = {
    server.stop()
    system.terminate()
  }

  private def createWatcher(probe: TestProbe) = new Watcher {
    override def process(we: WatchedEvent) = {
      probe.send(probe.ref, we)
    }
  }

  private def convertStringToBytes(input: String) = input.toCharArray().map { c => c.toByte }

  describe("ZooKeeper") {
    it("should receive connected event") {
      zooKeeper = new ZooKeeper(server.getConnectString, zooKeeperClientTimeout, createWatcher(testProbe))
      val msg = testProbe.expectMsgClass(new FiniteDuration(10, TimeUnit.SECONDS), classOf[WatchedEvent])
      info(s"WatchedEvent: $msg")
      info(s"SessionId: ${zooKeeper.getSessionId}")
      info(s"SessionTimeout: ${zooKeeper.getSessionTimeout}")
      assert(msg.getState() === KeeperState.SyncConnected)
      assert(zooKeeper.getState === ZooKeeper.States.CONNECTED)
    }
  }

  describe("ZooKeeper Ephemeral path") {
    val path = "/testEphemeral"

    it("should not have path created and registers a watcher to the path") {
      val stat = zooKeeper.exists(path, createWatcher(testProbe))
      info(s"stat: ${stat}")
      assert(stat == null)
    }

    val sampleData = "some sample data"

    it("should create path and receive node create event") {
      val realPath = zooKeeper.create(path, convertStringToBytes(sampleData), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL)
      info(s"realPath: ${realPath}")
      assert(realPath.equals(path))

      val msg = testProbe.expectMsgClass(new FiniteDuration(10, TimeUnit.SECONDS), classOf[WatchedEvent])
      info(s"WatchedEvent: $msg")
      assert(msg.getType == EventType.NodeCreated)
    }

    it("should change data but no event") {
      val stat = zooKeeper.setData(path, convertStringToBytes(sampleData), -1)
      info(s"stat: ${stat}")
      assert(stat != null)
      testProbe.expectNoMsg()
    }

    it("should have path created and registers a watcher") {
      val stat = zooKeeper.exists(path, createWatcher(testProbe))
      info(s"stat: ${stat}")
      assert(stat != null)
    }

    it("should change data and data changed event produced") {
      val stat = zooKeeper.setData(path, convertStringToBytes(sampleData), -1)
      info(s"stat: ${stat}")
      assert(stat != null)
      val msg = testProbe.expectMsgClass(new FiniteDuration(10, TimeUnit.SECONDS), classOf[WatchedEvent])
      info(s"WatchedEvent: $msg")
      assert(msg.getType == EventType.NodeDataChanged)
    }

    it("should get exception when a child is created") {
      val stat = zooKeeper.exists(path, createWatcher(testProbe))
      info(s"stat: ${stat}")
      assert(stat != null)

      val ex = intercept[Exception] {
        val realPath = zooKeeper.create(s"${path}/children1", convertStringToBytes("This is children1"),
          ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL)
      }

      info(s"ex: ${ex}")
    }
    
    ignore("should have closed connection after timeout") {
      val timeLeft = zooKeeperClientTimeout - System.currentTimeMillis()
      info(s"timeLeft: ${timeLeft}")
      if (timeLeft > 0)
        Thread.sleep(timeLeft + 1000) // Sleep for 1 more second
        
      assert(zooKeeper.getState === ZooKeeper.States.CLOSED)
    }
  }
  
  describe("ZooKeeper persistent path") {
    val path = "/testPersistent"

    it("should not have path created and registers a watcher to the path") {
      val stat = zooKeeper.exists(path, createWatcher(testProbe))
      info(s"stat: ${stat}")
      assert(stat == null)
    }
    
    val sampleData = "some sample data"

    it("should create path and receive node create event") {
      val realPath = zooKeeper.create(path, convertStringToBytes(sampleData), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT)
      info(s"realPath: ${realPath}")
      assert(realPath.equals(path))

      val msg = testProbe.expectMsgClass(new FiniteDuration(10, TimeUnit.SECONDS), classOf[WatchedEvent])
      info(s"WatchedEvent: $msg")
      assert(msg.getType == EventType.NodeCreated)
    }
    
    val childPath = s"${path}/children1"
    val childSampleData = "This is children1"
    
    it("should be able to create children and receive event") {
      val stat = zooKeeper.getChildren(path, createWatcher(testProbe))
      info(s"stat: ${stat}")
      assert(stat != null)

      val realPath = zooKeeper.create(childPath, convertStringToBytes(childSampleData), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT)
      info(s"realPath: ${realPath}")
      assert(realPath.equals(childPath))
      
      val msg = testProbe.expectMsgClass(new FiniteDuration(10, TimeUnit.SECONDS), classOf[WatchedEvent])
      info(s"WatchedEvent: $msg")
      assert(msg.getType == EventType.NodeChildrenChanged )
    }
    
    it("should get data from children path") {
      val childPath = s"${path}/children1"
      val stat = new Stat()
      val data = zooKeeper.getData(childPath, false, stat)
      val data2 = data.map { b => b.toChar }.mkString("")
      info(data2)
      assert(childSampleData.equals(data2))
    }
  }
}