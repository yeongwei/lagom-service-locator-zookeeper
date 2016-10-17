package persistent.npm.service.registry

object ServiceCharlieInstance1 extends SampleService {
  def main(args: Array[String]): Unit = {
    start(Array("sherpavm:2182", "/npi/services", "charlie", "1", "localhost", "9001", "true"))
  }
}