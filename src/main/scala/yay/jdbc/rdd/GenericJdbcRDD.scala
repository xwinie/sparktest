package yay.jdbc.rdd

import java.sql.{Connection, ResultSet}

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.{JdbcRDD, RDD}
import org.apache.spark.{Logging, Partition, SparkContext, TaskContext}

import scala.reflect.ClassTag

/**
  * Created by ${yuananyun} on 2015/2/7.
  */
private[yay] class JdbcPartition(idx: Int, val lower: Long, val upper: Long) extends Partition {
  override def index = idx
}

class GenericJdbcRDD[T: ClassTag](
                                   sc: SparkContext,
                                   getConnection: () => Connection,
                                   sql: String,
                                   paramsMap: Map[String, Object],
                                   skip: Long,
                                   take: Long,
                                   numPartitions: Int,
                                   mapRow: (ResultSet) => T = JdbcRDD.resultSetToObjectArray _) extends RDD[T](sc, Nil) with Logging {
  @DeveloperApi
  override def compute(thePart: Partition, context: TaskContext) = new JdbcNextIterator[T] {
    context.addTaskCompletionListener { context => closeIfNeeded()}
    val part = thePart.asInstanceOf[JdbcPartition]
    val conn = getConnection()
    //    val stmt = conn.prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)
    var parsedSql = ""
    if (conn.getMetaData.getURL.matches("jdbc:mysql:.*")) {
      parsedSql = sql+" limit "+ part.lower+","+ part.upper
    }

    val stmt = new NamedParameterStatement(conn, parsedSql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)

    // setFetchSize(Integer.MIN_VALUE) is a mysql driver specific way to force streaming results,
    // rather than pulling entire resultset into memory.
    // see http://dev.mysql.com/doc/refman/5.0/en/connector-j-reference-implementation-notes.html
    if (conn.getMetaData.getURL.matches("jdbc:mysql:.*")) {
      stmt.setFetchSize(Integer.MIN_VALUE)
      logInfo("statement fetch size set to: " + stmt.getFetchSize + " to force MySQL streaming ")
    }
    if (paramsMap != null && paramsMap.size > 0) {
      val paramsIter = paramsMap.iterator
      while (paramsIter.hasNext) {
        val (key, value) = paramsIter.next()
        stmt.setObject(key, value)
      }
    }
    val rs = stmt.executeQuery()

    override def getNext: T = {
      if (rs.next()) {
        mapRow(rs)
      } else {
        finished = true
        null.asInstanceOf[T]
      }
    }

    override def close() {
      try {
        if (null != rs && !rs.isClosed()) {
          rs.close()
        }
      } catch {
        case e: Exception => logWarning("Exception closing resultset", e)
      }
      try {
        if (null != stmt && !stmt.isClosed()) {
          stmt.close()
        }
      } catch {
        case e: Exception => logWarning("Exception closing statement", e)
      }
      try {
        if (null != conn && !conn.isClosed()) {
          conn.close()
        }
        logInfo("closed connection")
      } catch {
        case e: Exception => logWarning("Exception closing connection", e)
      }
    }
  }

  override protected def getPartitions: Array[Partition] = {
    take > 0 match {
      case false => throw new IllegalArgumentException("take 参数不能小于0")
      case _ => {
        val step = take / numPartitions
        (0 until numPartitions).map(i => {
          val start = (skip + i * step)
          val end = start + step
          new JdbcPartition(i, start, end)
        }).toArray
      }
    }
  }

}