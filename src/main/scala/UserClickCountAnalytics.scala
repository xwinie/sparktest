/**
  * Created by BingBee on 2016/3/2.
  */

import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import org.apache.spark.SparkConf
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream.toPairDStreamFunctions
import org.apache.spark.streaming.kafka.KafkaUtils

import kafka.serializer.StringDecoder
import net.sf.json.JSONObject
import redis.clients.jedis.JedisPool

object UserClickCountAnalytics {

  def main(args: Array[String]): Unit = {
    var masterUrl = "local[1]"
    if (args.length > 0) {
      masterUrl = args(0)
    }

    // Create a StreamingContext with the given master URL
    val conf = new SparkConf().setMaster(masterUrl).setAppName("UserClickCountStat")
    val ssc = new StreamingContext(conf, Seconds(5))

    // Kafka configurations
    val topics = Set("user_events")
    val brokers = "10.10.4.126:9092,10.10.4.127:9092"
    val kafkaParams = Map[String, String](
      "metadata.broker.list" -> brokers, "serializer.class" -> "kafka.serializer.StringEncoder")

    val dbIndex = 1
    val clickHashKey = "app::users::click"

    // Create a direct stream
    val kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)

    val events = kafkaStream.flatMap(line => {
      val data = JSONObject.fromObject(line._2)
      Some(data)
    })

    // Compute user click times
    val userClicks = events.map(x => (x.getString("uid"), x.getInt("click_count"))).reduceByKey(_ + _)
    userClicks.foreachRDD(rdd => {
      rdd.foreachPartition(partitionOfRecords => {
        partitionOfRecords.foreach(pair => {

          /**
            * Internal Redis client for managing Redis connection {@link Jedis} based on {@link RedisPool}
            */
          object InternalRedisClient extends Serializable {

            @transient private var pool: JedisPool = null

            def makePool(redisHost: String, redisPort: Int, redisTimeout: Int,
                         maxTotal: Int, maxIdle: Int, minIdle: Int): Unit = {
              makePool(redisHost, redisPort, redisTimeout, maxTotal, maxIdle, minIdle, true, false, 10000)
            }

            def makePool(redisHost: String, redisPort: Int, redisTimeout: Int,
                         maxTotal: Int, maxIdle: Int, minIdle: Int, testOnBorrow: Boolean,
                         testOnReturn: Boolean, maxWaitMillis: Long): Unit = {
              if(pool == null) {
                val poolConfig = new GenericObjectPoolConfig()
                poolConfig.setMaxTotal(maxTotal)
                poolConfig.setMaxIdle(maxIdle)
                poolConfig.setMinIdle(minIdle)
                poolConfig.setTestOnBorrow(testOnBorrow)
                poolConfig.setTestOnReturn(testOnReturn)
                poolConfig.setMaxWaitMillis(maxWaitMillis)
                pool = new JedisPool(poolConfig, redisHost, redisPort, redisTimeout)

                val hook = new Thread{
                  override def run = pool.destroy()
                }
                sys.addShutdownHook(hook.run)
              }
            }

            def getPool: JedisPool = {
              assert(pool != null)
              pool
            }
          }

          // Redis configurations
          val maxTotal = 10
          val maxIdle = 10
          val minIdle = 1
          val redisHost = "10.10.4.130"
          val redisPort = 6379
          val redisTimeout = 30000
          val dbIndex = 1
          InternalRedisClient.makePool(redisHost, redisPort, redisTimeout, maxTotal, maxIdle, minIdle)

          val uid = pair._1
          val clickCount = pair._2
          val jedis =InternalRedisClient.getPool.getResource
          jedis.select(dbIndex)
          jedis.hincrBy(clickHashKey, uid, clickCount)
          InternalRedisClient.getPool.returnResource(jedis)
        })
      })
    })

    ssc.start()
    ssc.awaitTermination()

  }
}