
import org.apache.spark.{SparkContext, SparkConf}
import org.json4s._
import org.json4s.jackson.JsonMethods._

object SaveCheckData {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("sparkToMysql").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val apirdds=
      """{"tableName":"B_PROBE_LOG","rowKey":"89b570:ec:e4:b7:7a:4492233705812445505681","informationValue":[
        |{"family":"B_PROBE_LOG", "qualifier":"PROBE_LOG_ID","value":"89b570:ec:e4:b7:7a:4492233705812445505681"},
        |{"family":"B_PROBE_LOG","qualifier":"MAC","value":"70:ec:e4:b7:7a:44"},
        |{"family":"B_PROBE_LOG","qualifier":"CONN_WIFI_EQUIP_ID","value":"1296D342D90482543982ADC3C4DD8CD3"},
        |{"family":"B_PROBE_LOG","qualifier":"CONN_WIFI_NAME","value":"王厨菜农老火锅"},
        |{"family":"B_PROBE_LOG","qualifier":"PROBE_TIME","value":"1455528657000"},
        |{"family":"B_PROBE_LOG","qualifier":"PROBE_AREA","value":"510105"},
        |{"family":"B_PROBE_LOG","qualifier":"LATITUDE","value":"35.972949"},
        |{"family":"B_PROBE_LOG","qualifier":"LONGITUDE","value":"120.170713"},
        |{"family":"B_PROBE_LOG","qualifier":"LATITUDE_LOGITUDE_TYPE","value":"10001,1"},
        |{"family":"B_PROBE_LOG","qualifier":"BUSINESS_AREA","value":"510199007"},
        |{"family":"B_PROBE_LOG","qualifier":"ONE_CATEGORY","value":"1001"},
        |{"family":"B_PROBE_LOG","qualifier":"TWO_CATEGORY","value":"1001001"},
        |{"family":"B_PROBE_LOG","qualifier":"SORUCE_APPID","value":"1455527324835"},
        |{"family":"B_PROBE_LOG","qualifier":"IS_TEST","value":"1001"}]} """.stripMargin
    val json = parse(apirdds)
   // println((json \ "informationValue"))
   // print(compact(render(json \ "informationValue")))
    val jsonrdd = sc.parallelize(
      compact(render(json \ "informationValue")) :: Nil)
    val informationValuerdd = sqlContext.read.json(jsonrdd)
    informationValuerdd.registerTempTable("informationValuerdd")
    val teenagers = sqlContext.sql("SELECT family, qualifier,value FROM informationValuerdd  where qualifier='LATITUDE_LOGITUDE_TYPE'")
    teenagers.show()
    val data= "string1,string2".split("\\,")
    val datardd = sc.parallelize(data.toSeq)
    print(datardd.collect().foreach(println))
  }
}