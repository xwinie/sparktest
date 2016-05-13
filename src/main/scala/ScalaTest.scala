import org.apache.spark.{SparkContext, SparkConf}

object ScalaTest {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("sparkToMysql").setMaster("local")

  }
}