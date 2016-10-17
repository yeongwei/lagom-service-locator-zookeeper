package persistent.npm.service.registry

object ServiceBetaInstance1 extends SampleService {
  def main(args: Array[String]): Unit = {
    start(Array("sherpavm:2182", "/npi/services", "beta", "1", "localhost", "8001", "true"))
  }
}