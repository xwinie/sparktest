import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.json4s._
import org.json4s.jackson.JsonMethods._

object LogStash {
  implicit val formats = DefaultFormats

  case class LogStashV1(message: String, path: String, host: String, lineno: Double, `@timestamp`: String)

  def main(args: Array[String]) {
    val Array(zkQuorum, group, topics, numThreads) = args
    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("LogStash")
    val sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc, Seconds(10))
    val lines = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap).map(_._2)
    lines.map(line => {
      val json = parse(line)
      json.extract[LogStashV1]
    }).print()
    ssc.start()
    ssc.awaitTermination()
  }
}