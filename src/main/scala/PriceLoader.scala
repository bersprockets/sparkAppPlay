import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object PriceLoader {

  def main(args: Array[String]) {
    if (args.length < 1) {
      Console.err.println("No input format specified")
      System.exit(1)
    }
    val inputFormat = args(0)

    if (args.length < 2) {
      Console.err.println("No input file specified")
      System.exit(1)
    }
    val inputFilename = args(1)

    if (args.length < 3) {
      Console.err.println("No output file specified")
      System.exit(2)
    }
    val outputDirectory = args(2)

    if (args.length < 4) {
      Console.err.println("No sec file specified")
      System.exit(3)
    }
    val secFilename = args(3)

    if (args.length < 5) {
      Console.err.println("No sec id external file specified")
      System.exit(4)
    }
    val securityIdExternalFilename = args(4)

    if (args.length < 6) {
      Console.err.println("No exchange specified")
      System.exit(5)
    }
    val exchangeFilename = args(5)

    val spark = SparkSession.builder().appName("PriceLoader").getOrCreate()
    import spark.implicits._

    val priceSchema = StructType(
      StructField("secIdExternal", StringType)
      :: StructField("externalIdTypeId", LongType)
      :: StructField("exchangeName", StringType)
      :: StructField("date", StringType)
      :: StructField("name", StringType)
      :: StructField("open", DoubleType)
      :: StructField("close", DoubleType)
      :: StructField("bid", DoubleType)
      :: StructField("offer", DoubleType)
      :: Nil
    )

    val priceDf = spark
      .read
      .schema(priceSchema)
      .format(inputFormat)
      .load(inputFilename)
      .select($"*", to_date($"date").as("date2"))

    val secSchema = StructType(
      StructField("id", LongType)
      :: StructField("name", StringType)
      :: StructField("country", StringType)
     :: Nil)

    val secDf = spark.read.schema(secSchema).format(inputFormat).load(secFilename)

    val secIdExternalSchema = StructType(
      StructField("secId", LongType)
      :: StructField("externalId", StringType)
      :: StructField("externalIdTypeId", LongType)
      :: StructField("country", StringType)
      :: StructField("startDate", StringType)
      :: StructField("endDate", StringType)
      :: Nil
    )

    val secIdExternalDf = spark.read.schema(secIdExternalSchema)
      .format(inputFormat)
      .load(securityIdExternalFilename)
      .select($"*",
        to_date($"startDate", "yyyy-MM-dd").as("startDate2"),
        to_date($"endDate", "yyyy-MM-dd").as("endDate2")
      )

    val exchangeSchema = StructType(
      StructField("id", LongType)
      :: StructField("name", StringType)
      :: StructField("country", StringType)
      :: Nil
    )

    val exchangeDf = spark.read.schema(exchangeSchema).format(inputFormat).load(exchangeFilename)

    val secPriceSchema = StructType(
      StructField("secId", LongType)
      :: StructField("exchangeId", LongType)
      :: StructField("date", StringType)
      :: StructField("priceType", StringType)
      :: StructField("price", DoubleType)
      :: Nil
    )

    val priceExchangeSplitDf = priceDf.select($"*", split('exchangeName, ":").as("exchangeSplit"))
    priceExchangeSplitDf.show

    // supplement price input with exchange information
    val priceExchangeDf = priceExchangeSplitDf.as("p")
      .join(exchangeDf.as("e"),
        $"p.exchangeSplit".getItem(0) === $"e.country"
          &&  $"p.exchangeSplit".getItem(1) === $"e.name")
      .select($"p.*", $"e.id".as("exchangeId"), $"e.country".as("country"))

    priceExchangeDf.show

    // resolve external ids in input file to internal ids
    val priceSecIdDf = priceExchangeDf.as("p")
      .join(secIdExternalDf.as("s"), $"p.secIdExternal" === $"s.externalId"
        && $"p.externalIdTypeId" ===  $"s.externalIdTypeId"
        && $"p.country" === $"s.country"
        && $"s.startDate2" <= $"p.date2"
        && $"s.endDate2" > $"p.date2", "left_outer")
      .select($"p.*", $"s.secId")
    // priceSecIdDf.show
    // println(priceSecIdDf.count)
    // println(priceSecIdDf.where(isnull('secId)).count)

    // find all external sec ids in price data where the external id has expired
    // but also has no newer external id of the same type
    val hasNewerSecIdExternalDf = priceSecIdDf
      .where(isnull('secId)).as("p")
      .join(secIdExternalDf.as("s"),
      $"p.secIdExternal" === $"s.externalId"
        && $"p.externalIdTypeId" ===  $"s.externalIdTypeId"
        && $"p.country" === $"s.country"
        && $"s.endDate2" <= $"p.date2")
      .select($"s.secId", $"s.externalId", $"s.externalIdTypeId", $"s.country", $"s.startDate2", $"s.endDate2")
      .as("s1")
      .join(secIdExternalDf.as("s2"), $"s1.secId" === $"s2.secId"
        && $"s1.externalIdTypeId" === $"s2.externalIdTypeId"
        && $"s1.country" === $"s2.country"
        && $"s1.endDate2" <= $"s2.startDate2")
      .select($"s1.secId", $"s1.externalId", $"s1.externalIdTypeId", $"s1.country", $"s1.endDate2", lit(true).as("bad"))
      .dropDuplicates

    // we cannot resolve the entries with ambiguous external ids. Mark such pricing entries as not usable
    val priceSecIdMarkedDf = priceSecIdDf.as("p")
      .join(hasNewerSecIdExternalDf.as("s"),
      $"p.secIdExternal" === $"s.externalId"
        && $"p.externalIdTypeId" ===  $"s.externalIdTypeId"
        && $"p.country" === $"s.country"
        && $"s.endDate2" <= $"p.date2", "left_outer")
      .select($"p.*", $"s.bad")

    // println(priceSecIdMarkedDf.where(!isnull('bad) && isnull('secId)).count)
    val priceSecIdCleanedDf = priceSecIdMarkedDf.where(isnull('bad) or !isnull('secId))

    // update all the null secId values with any external id that matches
    val priceReadyDf = priceSecIdCleanedDf
      .where(!isnull('secId))
      .select($"secIdExternal", $"externalIdTypeId", $"exchangeName", $"name", $"open", $"close",
        $"bid", $"offer", $"date2", $"exchangeId", $"country", $"secId")
    val priceNotReadyDf = priceSecIdCleanedDf.where(isnull('secId))

    println("priceReadyDf count " + priceReadyDf.count)
    println("priceNotReadyDf count " + priceNotReadyDf.count)

    // TODO: should really only join with secIdExternal entries with max endDate
    val priceNotReadySetDf = priceNotReadyDf.drop('secId).as("p")
      .join(secIdExternalDf.as("s"), $"p.secIdExternal" === $"s.externalId"
        && $"p.externalIdTypeId" ===  $"s.externalIdTypeId"
        && $"p.country" === $"s.country"
        && $"s.startDate2" <= $"p.date2")
      .select($"p.secIdExternal", $"p.externalIdTypeId", $"exchangeName", $"name", $"open", $"close",
        $"bid", $"offer", $"date2", $"exchangeId", $"p.country", $"s.secId")

    // priceReadyDf.printSchema
    // priceNotReadySet.printSchema

    // there might be some stragglers with secId not set to a good value
    // TODO: actually, this step not needed because of above inner join
    val priceNowReadyDf = priceNotReadySetDf.where(!isnull('secId))

    println(priceNotReadySetDf.count)
    println(priceNowReadyDf.count)

    val pricesSet = priceReadyDf.union(priceNowReadyDf)

    // produce sec_price output
    val secPriceOpenDf = pricesSet
      .select('secId, 'exchangeId, 'date2.as("date"), lit("open").as("priceType"), 'open.as("price"))
      .where('open > -0.1)
    secPriceOpenDf.printSchema
    println(secPriceOpenDf.count)

  }


}
