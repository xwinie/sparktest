package yay.jdbc.rdd

import java.sql.DriverManager

import org.apache.spark.SparkContext

object SparkToJDBC {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext("local", "mySql")
    val paramsMap = Map[String, Object]("msgType" -> "99", "sendTime" -> "1419821878146")
    val rdd = new GenericJdbcRDD(sc, () => {
      Class.forName("com.mysql.jdbc.Driver").newInstance()
      DriverManager.getConnection("jdbc:mysql://121.40.31.172:3306/ods?characterEncoding=utf-8", "root", "123.com")
    }, "SELECT * FROM api_in_column where msg_type=:msgType and send_time>=:sendTime",

      paramsMap, 0, 1000, 3, r => (r.getString(6),r.getString(11)))

    //    rdd.foreach(x => println(x))
    rdd.saveAsTextFile("c:\\temp\\test")
    sc.stop()
  }

}