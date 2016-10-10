package persistent.npm.service.registry

import akka.actor.ActorSystem
import akka.testkit.TestKit
import akka.testkit.TestProbe

import collection.JavaConverters._

import java.util.concurrent.TimeUnit

import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.framework.imps.CuratorFrameworkState
import org.apache.curator.framework.recipes.cache.PathChildrenCache
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener
import org.apache.curator.retry.RetryForever
import org.apache.curator.test.TestingServer
import org.scalatest.BeforeAndAfterAll
import org.scalatest.FunSpecLike
import org.apache.curator.framework.recipes.cache.TreeCache
import org.apache.curator.framework.recipes.cache.TreeCacheListener
import org.apache.curator.framework.recipes.cache.TreeCacheEvent

import scala.concurrent.duration.FiniteDuration
import java.util.concurrent.atomic.AtomicInteger

object CuratorFrameworkApiSpec extends BaseSpec {
  def name = "CuratorFrameworkApiSpec"
}

class CuratorFrameworkApiSpec extends TestKit(ActorSystem(CuratorFrameworkApiSpec.name, CuratorFrameworkApiSpec.configuration))
    with FunSpecLike with BeforeAndAfterAll {

  private val sessionTimeout = 5000
  private val connectionTimeout = 5000
  private val retryInterval = 3000
  private val testProbe = TestProbe()
  private val counter = new AtomicInteger(0)
  
  private var server: TestingServer = _
  private var client: CuratorFramework = _
  private var cache: TreeCache = _
  
  override def beforeAll = {
    server = CuratorFrameworkApiSpec.zooKeeperServer
    server.start()
  }
  override def afterAll = {
    if (client != null)
      client.close()

    server.close()
  }

  describe("Curator Framework API") {
    it("should connect to ZooKeeper server") {
      client = CuratorFrameworkFactory.newClient(server.getConnectString, sessionTimeout, connectionTimeout, new RetryForever(retryInterval))
      client.start()
      val startMs = System.currentTimeMillis()
      info(s"startMs: ${startMs}")
      client.blockUntilConnected()
      val endMs = System.currentTimeMillis()
      info(s"endMs: ${endMs}")
      info(s"elapsedMs: ${endMs - startMs}")
      assert(client.getState == CuratorFrameworkState.STARTED)
    }

    val path = "/testCurator"

    it("should start a TreeCache") {
      cache = new TreeCache(client, path)
      assert(true)
    }

    it("should add new listener") {
      val listener = new TreeCacheListener() {
        def childEvent(client: CuratorFramework, event: TreeCacheEvent) = {
          info(s"event[${counter.incrementAndGet()}]: ${event}")
          testProbe.send(testProbe.ref, event)
        }
      }
      cache.getListenable.addListener(listener)
      assert(true)
    }
    
    it("should get event after starting TreeCache") {
      cache.start()
      val msg = testProbe.expectMsgClass(new FiniteDuration(10, TimeUnit.SECONDS), classOf[TreeCacheEvent])
      info(s"WatchedEvent: $msg")
      assert(msg.getType == TreeCacheEvent.Type.INITIALIZED)
    }
    
    val sampleData = "This is testCurator"

    it("should create path") {
      val path1 = client.create().forPath(path)
      assert(path1.equals(path))
      
      val msg = testProbe.expectMsgClass(new FiniteDuration(10, TimeUnit.SECONDS), classOf[TreeCacheEvent])
      info(s"WatchedEvent: $msg")
      assert(msg.getType == TreeCacheEvent.Type.NODE_ADDED)
    }

    it("should write some data and get them back") {
      val stat = client.setData().forPath(path, CuratorFrameworkApiSpec.convertStringToBytes(sampleData))
      assert(stat != null)
      info(s"stat: ${stat}")

      val msg = testProbe.expectMsgClass(new FiniteDuration(10, TimeUnit.SECONDS), classOf[TreeCacheEvent])
      info(s"WatchedEvent: $msg")
      assert(msg.getType == TreeCacheEvent.Type.NODE_UPDATED)
      assert(CuratorFrameworkApiSpec.convertBytesToString(msg.getData.getData).equals(sampleData))
    }

    it("should receive event from adding new child") {
      val childPath = s"${path}/children1"
      val path1 = client.create().forPath(childPath)
      assert(path1.equals(childPath))

      val msg = testProbe.expectMsgClass(new FiniteDuration(10, TimeUnit.SECONDS), classOf[TreeCacheEvent])
      info(s"WatchedEvent: $msg")
      assert(msg.getType == TreeCacheEvent.Type.NODE_ADDED)
      assert(client.getChildren.forPath(path).asScala.size == 1)
    }

    it("should receive event from deleting child") {
      val childPath = s"${path}/children1"
      val path1 = client.delete().guaranteed().forPath(childPath)
      assert(client.getChildren.forPath(path).asScala.size == 0)

      val msg = testProbe.expectMsgClass(new FiniteDuration(10, TimeUnit.SECONDS), classOf[TreeCacheEvent])
      info(s"WatchedEvent: $msg")
      assert(msg.getType == TreeCacheEvent.Type.NODE_REMOVED)
      assert(client.getChildren.forPath(path).asScala.size == 0)
    }
  }
}