
import java.sql.DriverManager

import org.apache.spark.SparkContext
import org.apache.spark.rdd.JdbcRDD

/**
  * User: 过往记忆
  * Date: 14-9-10
  * Time: 下午13:16
  * bolg: http://www.iteblog.com
  * 本文地址：http://www.iteblog.com/archives/1113
  * 过往记忆博客，专注于hadoop、hive、spark、shark、flume的技术博客，大量的干货
  * 过往记忆博客微信公共帐号：iteblog_hadoop
  */
object SparkToJDBC {

  def main(args: Array[String]) {
    val sc = new SparkContext("local", "mysql")
    val rdd = new JdbcRDD(
      sc,
      () => {
        Class.forName("com.mysql.jdbc.Driver").newInstance()
        DriverManager.getConnection("jdbc:mysql://localhost:3306/db", "root", "123456")
      },
      "SELECT content FROM mysqltest WHERE ID >= ? AND ID <= ?",
      1, 100, 3,
      r => r.getString(1)).cache()

    print(rdd.filter(_.contains("success")).count())
    sc.stop()
  }
}