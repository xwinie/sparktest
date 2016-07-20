import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local")
    conf.setAppName("word count for big data")
    val sparkContext = new SparkContext(conf)
    val lines = sparkContext.parallelize(Seq("word count for big data"))
    lines.persist()
    lines.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _).collect().foreach(println)

  }

}
