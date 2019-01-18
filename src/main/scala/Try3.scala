import java.util

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Column

import scala.collection.JavaConverters

object Try3 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("GenerateWideDataFormat").enableHiveSupport().getOrCreate()

    val df = spark.read.schema("id1 int, id2 int").csv(args(0))
    val groupBySeq = Stream(col("id1"))
    df.groupBy(groupBySeq: _*).max("id2").toDF("id1", "id2").show()
  }
}
