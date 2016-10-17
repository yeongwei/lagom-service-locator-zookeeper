package persistent.npm.service.registry

import akka.actor.ActorSystem

trait SampleService extends BaseSpec {
  def name = "SampleService"

  def start(args: Array[String]): Unit = {
    val system = ActorSystem(name, configuration)
    system.actorOf(MockService.props(args(0), args(1), args(2), args(3), args(4),
      Integer.parseInt(args(5)),
      args(6).toBoolean))
  }
}