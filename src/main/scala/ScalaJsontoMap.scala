import scala.util.parsing.json.JSON


object ScalaJsontoMap {
  def main(args: Array[String]) {
    val str2 = "{\"et\":\"kanqiu_client_join\",\"vtm\":1435898329434,\"body\":{\"client\":\"866963024862254\",\"client_type\":\"android\",\"room\":\"NBA_HOME\",\"gid\":\"\",\"type\":\"\",\"roomid\":\"\"},\"time\":1435898329}"

  }

  def getchildjson(jsonsting :String,child:String)={
    val b = JSON.parseFull(jsonsting)
    val n=b match {
      case Some(map: Map[String, Any]) => map(child)
      case None => "Parsing failed"
      case other => "Unknown data structure: " + other
    }
    print(n)
  }
}