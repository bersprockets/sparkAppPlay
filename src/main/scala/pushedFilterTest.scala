import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

import org.apache.kudu.spark.kudu._
import org.apache.kudu.client._
import collection.JavaConverters._

object pushedFilterTest {
  case class Record(val randid2: Long, val randid1: Long)
  case class KuduRecord(val pk: Long, val randid1: Long, val randid2: Long)

  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      Console.err.println("No test type specified")
      System.exit(1)
    }

    val spark = SparkSession.builder().appName("PushedFilterTest").getOrCreate()
    val schema = StructType(StructField("randid1", LongType) :: StructField("randid2", LongType) :: Nil)

    args(0) match {
      case "dsJoinFilterGroupBy" =>
        dsJoinFilterGroupBy(args, spark, schema)
      case "dfJoinFilterGroupBy" =>
        dfJoinFilterGroupBy(args, spark, schema)
      case "dsJoinFilter" =>
        dsJoinFilter(args, spark, schema)
      case "dfJoinFilter" =>
        dfJoinFilter(args, spark, schema)
      case "dsFilter" =>
        dsFilter(args, spark, schema)
      case "dfFilter" =>
        dfFilter(args, spark, schema)
      case "dsJoinFilterGroupByKudu" =>
        dsJoinFilterGroupByKudu(args, spark)
      case "dfJoinFilterGroupByKudu" =>
        dfJoinFilterGroupByKudu(args, spark)
      case "dsJoinPreFilterGroupBy" =>
        dsJoinPreFilterGroupBy(args, spark, schema)
      case "dsJoinPreFilterGroupByKudu" =>
        dsJoinPreFilterGroupByKudu(args, spark)
      case _ => Console.err.println("What???")
    }

    spark.stop()
    spark.close()
  }


  def dsJoinFilterGroupBy(args: Array[String], spark: SparkSession, schema: StructType): Unit = {
      if (args.length < 2) {
        Console.err.println("No input file format specified")
        System.exit(1)
      }
      val format = args(1);

    if (args.length < 3) {
      Console.err.println("No left input file format specified")
      System.exit(1)
    }
    val leftInputFilename = args(2);

    if (args.length < 4) {
      Console.err.println("No right input file specified")
      System.exit(1)
    }
    val rightInputFilename = args(3);

    import spark.implicits._

    val table1 = spark.read.schema(schema).format(format).load(leftInputFilename).as[Record]
    val table2 = spark.read.schema(schema).format(format).load(rightInputFilename).as[Record]

    val resultDs = table1
      .joinWith(table2, table1.col("randid1") === table2.col("randid2"))
      .filter("_2.randid2 > 30")
      .groupBy("_1.randid1")
      .agg(count("*") as "numOccurances")

    resultDs.explain

    val startTime = System.currentTimeMillis()
    println(resultDs.count)
    val interval = System.currentTimeMillis() - startTime
    println(s"Interval is $interval")
  }

  def dfJoinFilterGroupBy(args: Array[String], spark: SparkSession, schema: StructType): Unit = {
    if (args.length < 2) {
      Console.err.println("No input file format specified")
      System.exit(1)
    }
    val format = args(1);

    if (args.length < 3) {
      Console.err.println("No left input file specified")
      System.exit(1)
    }
    val leftInputFilename = args(2);

    if (args.length < 4) {
      Console.err.println("No right input file specified")
      System.exit(1)
    }
    val rightInputFilename = args(3);

    import spark.implicits._

    val table1 = spark.read.schema(schema).format(format).load(leftInputFilename)
    val table2 = spark.read.schema(schema).format(format).load(rightInputFilename)

    val resultDf = table1
      .join(table2,  table1.col("randid1") === table2.col("randid2"))
      .filter(table2("randid2") >"30")
      .groupBy(table1("randid1"))
      .agg(count("*") as "numOccurances")

    resultDf.explain

    val startTime = System.currentTimeMillis()
    println(resultDf.count)
    val interval = System.currentTimeMillis() - startTime
    println(s"Interval is $interval")
  }

  def dsJoinFilter(args: Array[String], spark: SparkSession, schema: StructType): Unit = {
    if (args.length < 2) {
      Console.err.println("No input file format specified")
      System.exit(1)
    }
    val format = args(1);

    if (args.length < 3) {
      Console.err.println("No left input file format specified")
      System.exit(1)
    }
    val leftInputFilename = args(2);

    if (args.length < 4) {
      Console.err.println("No right input file specified")
      System.exit(1)
    }
    val rightInputFilename = args(3);

    import spark.implicits._

    val table1 = spark.read.schema(schema).format(format).load(leftInputFilename).as[Record]
    val table2 = spark.read.schema(schema).format(format).load(rightInputFilename).as[Record]

    val resultDs = table1
      .joinWith(table2, table1.col("randid1") === table2.col("randid2"))
      .filter("_2.randid2 > 30")

    resultDs.explain

    val startTime = System.currentTimeMillis()
    println(resultDs.count)
    val interval = System.currentTimeMillis() - startTime
    println(s"Interval is $interval")
  }

  def dfJoinFilter(args: Array[String], spark: SparkSession, schema: StructType): Unit = {
    if (args.length < 2) {
      Console.err.println("No input file format specified")
      System.exit(1)
    }
    val format = args(1);

    if (args.length < 3) {
      Console.err.println("No left input file specified")
      System.exit(1)
    }
    val leftInputFilename = args(2);

    if (args.length < 4) {
      Console.err.println("No right input file specified")
      System.exit(1)
    }
    val rightInputFilename = args(3);

    import spark.implicits._

    val table1 = spark.read.schema(schema).format(format).load(leftInputFilename)
    val table2 = spark.read.schema(schema).format(format).load(rightInputFilename)

    val resultDf = table1
      .join(table2, table1.col("randid1") === table2.col("randid2"))
      .filter(table2("randid2") >"30")

    resultDf.explain

    val startTime = System.currentTimeMillis()
    println(resultDf.count)
    val interval = System.currentTimeMillis() - startTime
    println(s"Interval is $interval")
  }

  def dsFilter(args: Array[String], spark: SparkSession, schema: StructType): Unit = {
    if (args.length < 2) {
      Console.err.println("No input file format specified")
      System.exit(1)
    }
    val format = args(1);

    if (args.length < 3) {
      Console.err.println("No input file specified")
      System.exit(1)
    }
    val inputFilename = args(2);

    import spark.implicits._

    val table1 = spark.read.schema(schema).format(format).load(inputFilename).as[Record]

    val resultDs = table1
      .filter("randid2 > 30")

    resultDs.explain

    val startTime = System.currentTimeMillis()
    println(resultDs.count)
    val interval = System.currentTimeMillis() - startTime
    println(s"Interval is $interval")
  }

  def dfFilter(args: Array[String], spark: SparkSession, schema: StructType): Unit = {
    if (args.length < 2) {
      Console.err.println("No input file format specified")
      System.exit(1)
    }
    val format = args(1);

    if (args.length < 3) {
      Console.err.println("No left input file specified")
      System.exit(1)
    }
    val inputFilename = args(2);

    import spark.implicits._

    val table1 = spark.read.schema(schema).format(format).load(inputFilename)

    val resultDf = table1
      .filter(table1("randid2") > "30")

    resultDf.explain

    val startTime = System.currentTimeMillis()
    println(resultDf.count)
    val interval = System.currentTimeMillis() - startTime
    println(s"Interval is $interval")
  }

  def dsJoinFilterGroupByKudu(args: Array[String], spark: SparkSession): Unit = {
    if (args.length < 2) {
      Console.err.println("No kudu master specified")
      System.exit(1)
    }
    val master = args(1);

    if (args.length < 3) {
      Console.err.println("No left kudu table specified")
      System.exit(1)
    }
    val leftTableName = args(2);

    if (args.length < 4) {
      Console.err.println("No right kudu table specified")
      System.exit(1)
    }
    val rightTableName = args(3);

    import spark.implicits._

    val table1Df = spark
      .read
      .options(Map("kudu.master" -> master, "kudu.table" -> leftTableName))
      .kudu
    val table1 = table1Df.as[KuduRecord]

    val table2Df = spark
      .read
      .options(Map("kudu.master" -> master, "kudu.table" -> rightTableName))
      .kudu
    val table2 = table2Df.as[KuduRecord]

    val resultDs = table1
      .joinWith(table2, table1.col("randid1") === table2.col("randid2"))
      .filter("_2.randid2 > 30")
      .groupBy("_1.randid1")
      .agg(count("*") as "numOccurances")

    resultDs.explain

    val startTime = System.currentTimeMillis()
    println(resultDs.count)
    val interval = System.currentTimeMillis() - startTime
    println(s"Interval is $interval")
  }

  def dfJoinFilterGroupByKudu(args: Array[String], spark: SparkSession): Unit = {
    if (args.length < 2) {
      Console.err.println("No kudu master specified")
      System.exit(1)
    }
    val master = args(1);

    if (args.length < 3) {
      Console.err.println("No left kudu table specified")
      System.exit(1)
    }
    val leftTableName = args(2);

    if (args.length < 4) {
      Console.err.println("No right kudu table specified")
      System.exit(1)
    }
    val rightTableName = args(3);

    import spark.implicits._

    val table1 = spark
      .read
      .options(Map("kudu.master" -> master, "kudu.table" -> leftTableName))
      .kudu

    val table2 = spark
      .read
      .options(Map("kudu.master" -> master, "kudu.table" -> rightTableName))
      .kudu

    val resultDf = table1
      .join(table2, table1.col("randid1") === table2.col("randid2"))
      .filter(table2("randid2") >"30")
      .groupBy(table1("randid1"))
      .agg(count("*") as "numOccurances")

    resultDf.explain

    val startTime = System.currentTimeMillis()
    println(resultDf.count)
    val interval = System.currentTimeMillis() - startTime
    println(s"Interval is $interval")
  }

  def dsJoinPreFilterGroupBy(args: Array[String], spark: SparkSession, schema: StructType): Unit = {
    if (args.length < 2) {
      Console.err.println("No input file format specified")
      System.exit(1)
    }
    val format = args(1);

    if (args.length < 2) {
      Console.err.println("No left input file format specified")
      System.exit(1)
    }
    val leftInputFilename = args(2);

    if (args.length < 4) {
      Console.err.println("No right input file specified")
      System.exit(1)
    }
    val rightInputFilename = args(3);

    import spark.implicits._

    val table1 = spark.read.schema(schema).format(format).load(leftInputFilename).as[Record]
    val table2 = spark.read.schema(schema).format(format).load(rightInputFilename).as[Record]

    val table1Filtered = table1.filter("randid1 > 30")
    val table2Filtered = table2.filter("randid2 > 30")
    val resultDs = table1Filtered
      .joinWith(table2Filtered, table1Filtered.col("randid1") === table2Filtered.col("randid2") )
      .groupBy("_1.randid1")
      .agg(count("*") as "numOccurances")

    resultDs.explain

    val startTime = System.currentTimeMillis()
    println(resultDs.count)
    val interval = System.currentTimeMillis() - startTime
    println(s"Interval is $interval")
  }

  def dsJoinPreFilterGroupByKudu(args: Array[String], spark: SparkSession): Unit = {
    if (args.length < 2) {
      Console.err.println("No kudu master specified")
      System.exit(1)
    }
    val master = args(1);

    if (args.length < 3) {
      Console.err.println("No left kudu table specified")
      System.exit(1)
    }
    val leftTableName = args(2);

    if (args.length < 4) {
      Console.err.println("No right kudu table specified")
      System.exit(1)
    }
    val rightTableName = args(3);

    import spark.implicits._

    val table1Df = spark
      .read
      .options(Map("kudu.master" -> master, "kudu.table" -> leftTableName))
      .kudu
    val table1 = table1Df.as[KuduRecord]

    val table2Df = spark
      .read
      .options(Map("kudu.master" -> master, "kudu.table" -> rightTableName))
      .kudu
    val table2 = table2Df.as[KuduRecord]

    val table1Filtered = table1.filter("randid1 > 30")
    val table2Filtered = table2.filter("randid2 > 30")
    val resultDs = table1Filtered
      .joinWith(table2Filtered, table1Filtered.col("randid1") === table2Filtered.col("randid2") )
      .groupBy("_1.randid1")
      .agg(count("*") as "numOccurances")

    resultDs.explain

    val startTime = System.currentTimeMillis()
    println(resultDs.count)
    val interval = System.currentTimeMillis() - startTime
    println(s"Interval is $interval")
  }
}
