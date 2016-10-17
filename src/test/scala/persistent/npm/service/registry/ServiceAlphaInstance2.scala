package persistent.npm.service.registry

object ServiceAlphaInstance2 extends SampleService {
  def main(args: Array[String]): Unit = {
    start(Array("sherpavm:2182", "/npi/services", "alpha", "2", "localhost", "7002", "true"))
  }
}