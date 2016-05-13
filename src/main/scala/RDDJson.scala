import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by BingBee on 2016/3/7.
  */
object RDDJson {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("sparkToMysql").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val anotherPeopleRDD = sc.parallelize(
      """{"name":"Yin","address":{"city":"Columbus","state":"Ohio"}}""" :: Nil)
    val anotherPeople = sqlContext.read.json(anotherPeopleRDD)
    anotherPeople.registerTempTable("anotherPeople")
    val anotherPeopleinfoRDD = sc.parallelize(
      """{"name":"Yin","age":"30"}""" :: Nil)
    val anotherPeopleinfo = sqlContext.read.json(anotherPeopleinfoRDD)
    anotherPeopleinfo.registerTempTable("anotherPeopleinfo")
    // anotherPeople.printSchema()
    val teenagers = sqlContext.sql("SELECT b.name,b.age FROM anotherPeople a  join  anotherPeopleinfo b on a.name=b.name  WHERE a.address.city='Columbus'")
    teenagers.show()
  }
}
