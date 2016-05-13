import java.sql.{DriverManager, PreparedStatement, Connection}
import org.apache.spark.{SparkContext, SparkConf}

object sparkToMysql {

  case class info(info1: String, info2: Int)

  def toMySQL(iterator: Iterator[(String, Int)]): Unit = {
    var conn: Connection = null
    var ps: PreparedStatement = null
    val sql = "insert into info(info1, info2) values (?, ?)"
    try {
      Class.forName("com.mysql.jdbc.Driver");
      conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/spark_test", "root", "111111")
      iterator.foreach(dataIn => {
        ps = conn.prepareStatement(sql)
        ps.setString(1, dataIn._1)
        ps.setInt(2, dataIn._2)
        ps.executeUpdate()
      }
      )
    } catch {
      case e: Exception => e.printStackTrace()
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
    val conf = new SparkConf().setAppName("sparkToMysql").setMaster("local")
    val sc = new SparkContext(conf)
    val dataFromHDFS=sc.textFile(args(0)).map(_.split("\\^")).map(line => (line(0),line(1).toInt))
    dataFromHDFS.foreachPartition(toMySQL)
  }
}