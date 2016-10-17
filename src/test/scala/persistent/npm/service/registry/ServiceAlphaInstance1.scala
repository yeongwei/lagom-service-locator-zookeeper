package persistent.npm.service.registry

object ServiceAlphaInstance1 extends SampleService {
  def main(args: Array[String]): Unit = {
    start(Array("sherpavm:2182", "/npi/services", "alpha", "1", "localhost", "7001", "true"))
  }
}