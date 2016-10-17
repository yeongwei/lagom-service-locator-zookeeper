package persistent.npm.service.registry

object ServiceBetaInstance2 extends SampleService {
  def main(args: Array[String]): Unit = {
    start(Array("sherpavm:2182", "/npi/services", "beta", "2", "localhost", "8002", "true"))
  }
}