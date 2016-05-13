/**
  * User: 过往记忆
  * Date: 15-03-10
  * Time: 上午07:30
  * bolg: http://www.iteblog.com
  * 本文地址：http://www.iteblog.com/archives/1275
  * 过往记忆博客，专注于hadoop、hive、spark、shark、flume的技术博客，大量的干货
  * 过往记忆博客微信公共帐号：iteblog_hadoop
  */

import java.sql.{DriverManager, PreparedStatement, Connection}
import org.apache.spark.{SparkContext, SparkConf}

object RDDtoMysql {

  case class Blog(name: String, count: Int)

  def myFun(iterator: Iterator[(String, Int)]): Unit = {
    var conn: Connection = null
    var ps: PreparedStatement = null
    val sql = "insert into blog(name, count) values (?, ?)"
    try {
      conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/spark", "root", "123456")
      iterator.foreach(data => {
        ps = conn.prepareStatement(sql)
        ps.setString(1, data._1)
        ps.setInt(2, data._2)
        ps.executeUpdate()
      }
      )
    } catch {
      case e: Exception => println("Mysql Exception")
    } finally {
      if (ps != null) {
        ps.close()
      }
      if (conn != null) {
        conn.close()
      }
    }
  }

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("RDDToMysql").setMaster("local")
    val sc = new SparkContext(conf)
    val data = sc.parallelize(List(("www", 10), ("iteblog", 20), ("com", 30)))
    data.foreachPartition(myFun)
  }
}