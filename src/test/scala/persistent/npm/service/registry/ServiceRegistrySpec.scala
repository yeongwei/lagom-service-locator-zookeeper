package persistent.npm.service.registry

import org.scalatest.FunSpec
import spray.json.JsonParser

class ServiceRegistrySpec extends FunSpec with ServiceInstanceJson {
  import ServiceInstanceProtocol._
  
  describe("Service Instance Json") {
    it("should be parsable") {
      val json = """
        {
         "name":"service1","id":"1","address":"sherpavm","port":9009,"sslPort":null,"payload":null,
    	   "registrationTimeUTC":1476357809874,"serviceType":"DYNAMIC",
         "uriSpec":{
         "parts":[
            {"value":"scheme","variable":true},
            {"value":"://","variable":false},
            {"value":"serviceAddress","variable":true},
            {"value":":","variable":false},
            {"value":"servicePort","variable":true}]}}
        """
      
      val serviceInstance = JsonParser(json).convertTo[ServiceInsance]
      assert(serviceInstance.name.equals("service1"))
      assert(serviceInstance.uriSpec.parts.size == 5)
    }
  }
}