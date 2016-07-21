import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SQLContext}

/**
  * Created by winie on 7/21/2016.
  */
object DataFrameParquetFile {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("TestUDF").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val row1 = Row("Bruce Zhang", "developer", 38 )
    val row2 = Row("Zhang Yi", "engineer", 39)
    val table = List(row1, row2)
    val rows = sc.parallelize(table)

    import org.apache.spark.sql.types._
    val schema = StructType(Array(StructField("name", StringType, true),StructField("role", StringType, true), StructField("age", IntegerType, true)))
    sqlContext.createDataFrame(rows, schema).registerTempTable("employee")
    sqlContext.sql("select * from employee").saveAsParquetFile("employee.parquet")
  }
}
