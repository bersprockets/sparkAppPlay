
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DoubleType, StructType}

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

object GenerateWideDataFormat {
  def main(args: Array[String]): Unit = {

    if (args.length < 1) {
      Console.err.println("No table name provided")
      sys.exit(1)
    }
    val tableName = args(0)

    if (args.length < 2) {
      Console.err.println("No column count provided")
      sys.exit(1)
    }
    val colCount = args(1).toInt

    if (args.length < 3) {
      Console.err.println("No row count provided")
      sys.exit(1)
    }
    val rowCount = args(2).toInt

    if (args.length < 4) {
      Console.err.println("No split count provided")
      sys.exit(1)
    }
    val splitCount = args(3).toInt

    if (args.length < 5) {
      Console.err.println("No csv file name provided")
      sys.exit(1)
    }
    val csvFilename = args(4)

    if (args.length < 6) {
      Console.err.println("No format provided")
      sys.exit(1)
    }
    val format = args(5)

    val dsOptions = if (args.length >= 7 && args(6).contains("=")) {
      println(args(6).getClass.getName)
      val opts = args(6).split(",")
      val tokenizedOptions = opts.map { opt =>
        val tokens = opt.split("=")
        (tokens(0),tokens(1))
      }
      tokenizedOptions.toMap
    } else {
      Map[String, String]()
    }

    println(dsOptions)

    val spark = SparkSession.builder().appName("GenerateWideDataFormat").enableHiveSupport().getOrCreate()
    // spark.sqlContext.setConf("parquet.dictionary.page.size", "209715")
    // spark.sqlContext.setConf("parquet.block.size", "25165824")

    val schemaOpt  = try {
      Some(spark.table(tableName).schema)
    } catch {
      case _: Exception => None
    }

    val tableExists = if (schemaOpt.isDefined) {
      true
    } else {
      false
    }

    val schema = if (schemaOpt.isDefined) {
      schemaOpt.get
    } else {
      var schemaTemp = StructType(Nil)
      for (i <- 1 to colCount) {
        schemaTemp = schemaTemp.add(s"id$i", DoubleType)
      }
      schemaTemp
    }

    var tableExistsNow = tableExists
    (0 until splitCount).foreach { i =>
      insertData(spark, schema, tableName, rowCount, csvFilename, tableExistsNow, format, dsOptions)
      tableExistsNow = true
    }
  }

  def insertData(
       spark: SparkSession,
       schema: StructType,
       tableName: String,
       rowCount: Int,
       csvFilenameBase: String,
       tableExists: Boolean,
       format: String,
       dsOptions: Map[String,String]): Unit = {

    val rand = Random

    val rows = ArrayBuffer.empty[String]
    (0 until rowCount).foreach { i =>
      val values = new StringBuffer()
      val response = ((rand.nextInt().abs % 425) + 1).toDouble
      values.append(response)
      (0 until schema.size-1).foreach { j =>
        values.append(",")
        if ((rand.nextInt().abs % 1000) < 38) {
          values.append(rand.nextDouble() % 7777)
        } else {
          // do nothing
        }
      }
      rows += values.toString
    }

    val csvFilename = csvFilenameBase + rand.alphanumeric.take(5).mkString

    val sc = spark.sparkContext
    val rdd = sc.parallelize(rows)
    println(s"Saving to file ${csvFilename}")
    rdd.saveAsTextFile(csvFilename)

    val df = spark.read.schema(schema).csv(csvFilename).coalesce(1)
    if (!tableExists) {
      println("Saving as table for the first time")
      df.write.format(format).options(dsOptions).saveAsTable(tableName)
    } else {
      println("Appending to table")
      df.write.format(format).mode("append").options(dsOptions).insertInto(tableName)
    }
  }
}
