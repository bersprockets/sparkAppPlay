
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DoubleType, StructType}

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

object GenerateWideDataDF {
  def main(args: Array[String]): Unit = {

    if (args.length < 1) {
      Console.err.println("No output path provided")
      sys.exit(1)
    }
    val outputPath = args(0)

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
      println(s"Length of opts is ${opts.size}")
      val tokenizedOptions = opts.map { opt =>
        val tempTokens = opt.split("=")
        if (tempTokens.size == 1) {
          (tempTokens(0), "")
        } else {
          (tempTokens(0), tempTokens(1))
        }
      }
      tokenizedOptions.toMap
    } else {
      Map[String, String]()
    }

    println(dsOptions)

    val spark = SparkSession.builder()
      .appName("GenerateWideDataDF")
      .enableHiveSupport()
      .getOrCreate()
    // spark.sqlContext.setConf("parquet.dictionary.page.size", "209715")
    // spark.sqlContext.setConf("parquet.block.size", "25165824")

    val schemaOpt  = try {
      Some(spark.read.format(format).load(outputPath).schema)
    } catch {
      case _: Exception => None
    }

    val tableExists = schemaOpt.isDefined

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
      insertData(spark, schema, outputPath, rowCount, csvFilename, tableExistsNow, format, dsOptions)
      tableExistsNow = true
    }
  }

  def insertData(
       spark: SparkSession,
       schema: StructType,
       outputPath: String,
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
      df.write.format(format).options(dsOptions).save(outputPath)
    } else {
      println("Appending to table")
      df.write.format(format).mode("append").options(dsOptions).save(outputPath)
    }
  }
}