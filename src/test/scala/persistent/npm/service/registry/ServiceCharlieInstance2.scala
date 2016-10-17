package persistent.npm.service.registry

object ServiceCharlieInstance2 extends SampleService {
  def main(args: Array[String]): Unit = {
    start(Array("sherpavm:2182", "/npi/services", "charlie", "2", "localhost", "9002", "true"))
  }
}