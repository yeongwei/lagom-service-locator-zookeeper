package persistent.npm.service.registry

import akka.actor.ActorSystem

object MockServiceRegistry extends BaseSpec {
  def name = "MockService"

  def main(args: Array[String]): Unit = {
    val system = ActorSystem(name, configuration)
    system.actorOf(ServiceRegistry.props("sherpavm:2182", "/npi/services"))
  }
}