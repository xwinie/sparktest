import com.mongodb.spark.MongoSpark
import com.mongodb.spark.config.ReadConfig
import org.apache.spark.{SparkConf, SparkContext}
import com.mongodb.spark._
import org.bson.Document

/**
  * Created by winie on 7/20/2016.
  */

object ScalConnMongo {
  def main(args: Array[String]) {
    val uri: String = args.headOption.getOrElse("mongodb://192.168.10.241/mfm.sys_dict")
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("MongoSparkConnectorTour")
    //      .set("spark.app.id", "MongoSparkConnectorTour")
    //      .set("spark.mongodb.input.uri", uri)
    //      .set("spark.mongodb.output.uri", uri)
    val sc = new SparkContext(conf)
    //    val readConfig = ReadConfig(Map("collection" -> "spark", "readPreference.name" -> "secondaryPreferred"), Some(ReadConfig(sc)))
    //    val customRdd = MongoSpark.load(sc,readConfig)
    //    //sc.loadFromMongoDB(ReadConfig(Map("uri" -> "mongodb://example.com/database.collection"))) // Uses the ReadConfig
    //    println(customRdd.count)
    ////    println(customRdd.first.toJson)
    //
    //    val docs = """
    //  {"name": "Bilbo Baggins", "age": 50}
    //  {"name": "Gandalf", "age": 1000}
    //  {"name": "Thorin", "age": 195}
    //  {"name": "Balin", "age": 178}
    //  {"name": "Kíli", "age": 77}
    //  {"name": "Dwalin", "age": 169}
    //  {"name": "Óin", "age": 167}
    //  {"name": "Glóin", "age": 158}
    //  {"name": "Fíli", "age": 82}
    //  {"name": "Bombur"}""".trim.stripMargin.split("[\\r\\n]+").toSeq
    //    sc.parallelize(docs.map(Document.parse)).saveToMongoDB()
    //    val readConfig = ReadConfig(Map("spark.mongodb.input.uri"->uri,"spark.mongodb.output.uri"->uri))
    //    val df = MongoSpark.load(sc,readConfig) // Uses the SparkConf
    ////    println(df.count())
    //    println(df.first.toJson)
    val df3 = sc.loadFromMongoDB(ReadConfig(Map("uri" -> "mongodb://192.168.10.241/mfm.sys_dict"))).toDF() // Uses the ReadConfig
    //    df3.filter(df3("type") < 110).show()
    df3.filter(df3("type") < 110).show()
    //    println(df3.first.toJson)
  }
}
