import org.apache.spark.sql.{SaveMode, SparkSession}
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
    val outputDirectoryName = args(2)

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
    // priceExchangeSplitDf.show

    // supplement price input with exchange information
    val priceExchangeDf = priceExchangeSplitDf.as("p")
      .join(exchangeDf.as("e"),
        $"p.exchangeSplit".getItem(0) === $"e.country"
          &&  $"p.exchangeSplit".getItem(1) === $"e.name")
      .select($"p.*", $"e.id".as("exchangeId"), $"e.country".as("country"))

    // priceExchangeDf.show

    // resolve external ids in input file to internal ids
    val priceSecIdDf = priceExchangeDf.as("p")
      .join(secIdExternalDf.as("s"),
        $"p.secIdExternal" === $"s.externalId"
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
    // TODO: replace this crazy mark-and-delete code with an except
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

    // println("priceSecIdMarkedDf count is " + priceSecIdMarkedDf.count)
    // println("priceSecIdMarkedDf bad count is " + priceSecIdMarkedDf.where(!isnull('bad) && isnull('secId)).count)
    val priceSecIdCleanedDf = priceSecIdMarkedDf.where(isnull('bad) || !isnull('secId))
    // println("priceSecIdCleanedDf count is " + priceSecIdCleanedDf.count)
    // println("priceSecIdCleanedDf unset count is " + priceSecIdCleanedDf.where(isnull('secId)).count)

    // update all the null secId values with any external id that matches
    val priceReadyDf = priceSecIdCleanedDf
      .where(!isnull('secId))
      .select($"secIdExternal", $"externalIdTypeId", $"exchangeName", $"name", $"open", $"close",
        $"bid", $"offer", $"date2", $"exchangeId", $"country", $"secId")
    val priceNotReadyDf = priceSecIdCleanedDf.where(isnull('secId))
    //  println("priceNotReadyDf distinct ids " + priceNotReadyDf.select('secIdExternal, 'externalIdTypeId, 'country)
    //  .dropDuplicates.count)

    priceNotReadyDf.select('secIdExternal, 'externalIdTypeId, 'country).dropDuplicates.show(75)

    // find secIdExternal entries that are not really expired and bring the back to life
    // Use except here!!
    val toReanimateDf = priceNotReadyDf.as("p")
      .join(secIdExternalDf.as("s"),
        $"p.secIdExternal" === $"s.externalId"
          && $"p.externalIdTypeId" ===  $"s.externalIdTypeId"
          && $"p.country" === $"s.country"
          && $"s.endDate2" <= $"p.date2")
      .select($"s.*", lit(true).as("notDead"))
      .dropDuplicates

    // for some reason must join with toReanimateDf on the left, otherwise we get
    // an error from a query earlier in the lineage.
    // This kills any chance of using except(), which would require secIdExternalDf
    // on the left
    val secIdExternalMarked = toReanimateDf.as("r")
      .join(secIdExternalDf.as("s"),
        $"s.secId" === $"r.secId"
          && $"s.externalId" === $"r.externalId"
          && $"s.externalIdTypeId" === $"r.externalIdTypeId"
          && $"s.country" === $"r.country"
          && $"s.startDate2" === $"r.startDate2"
          && $"s.endDate2" === $"r.endDate2", "right_outer")
      .select($"s.*", $"r.notDead")

    val secIdExternalCleanedDf = secIdExternalMarked.where(isnull('notDead)).drop('notDead)
    val secIdExternalNowReadyDf = secIdExternalMarked
      .where(!isnull('notDead))
      .drop('notDead)
      .drop('endDate)
      .drop('endDate2).as("s")
      .select($"s.*", lit("2299-12-31").as("endDate"),
        to_date(lit("2299-12-31"), "yyyy-MM-dd").as("endDate2"))

    // union might work incorrectly but also not fail when columns
    // are not in the exact same order but the mismatching columns just
    // happen to have the same type!!!
    val newSecIdExternalDf = secIdExternalCleanedDf
      .select('secId, 'externalId, 'externalIdTypeId, 'country, 'startDate, 'endDate, 'startDate2, 'endDate2)
      .union(secIdExternalNowReadyDf
        .select('secId, 'externalId, 'externalIdTypeId, 'country, 'startDate, 'endDate, 'startDate2, 'endDate2))
    // val newSecIdExternalDf = secIdExternalCleanedDf.union(secIdExternalNowReadyDf)
    secIdExternalNowReadyDf.where('externalId === "GB1066858894").show
    newSecIdExternalDf.where('externalId === "GB1066858894").show
    priceNotReadyDf.where('secIdExternal === "GB1066858894").show

    /* val existingCorrectSecIdExternal = secIdExternalDf
      .except(toReanimateDf) */

    // println("secIdExternalDf count " + secIdExternalDf.count)
    // println("existingCorrectSecIdExternal count " + existingCorrectSecIdExternal.count)

    // TODO: should really only join with secIdExternal entries with max endDate
    val priceNotReadySetDf = priceNotReadyDf.drop('secId).as("p")
      .join(newSecIdExternalDf.as("s"),
        $"p.secIdExternal" === $"s.externalId"
        && $"p.externalIdTypeId" ===  $"s.externalIdTypeId"
        && $"p.country" === $"s.country"
        && $"s.startDate2" <= $"p.date2"
        && $"s.endDate2" > $"p.date2")
      .select($"p.secIdExternal", $"p.externalIdTypeId", $"exchangeName", $"name", $"open", $"close",
        $"bid", $"offer", $"date2", $"exchangeId", $"p.country", $"s.secId")
    priceNotReadySetDf.where('secIdExternal === "GB1066858894").show

    // there might be some stragglers with secId not set to a good value
    // TODO: actually, this step not needed because of above inner join
    val priceNowReadyDf = priceNotReadySetDf.where(!isnull('secId))
    priceNotReadyDf.persist
    priceNotReadySetDf.persist
    priceNowReadyDf.persist
    println("priceNotReadyDf count is " + priceNotReadySetDf.count)
    println("priceNotReadySetDf count is " + priceNotReadySetDf.count)
    println("priceNowReadyDf count is " + priceNowReadyDf.count)

    val pricesSet = priceReadyDf.union(priceNowReadyDf)

    // produce sec_price output
    val secPriceOpenDf = pricesSet
      .select('secId, 'exchangeId, 'date2.as("date"), lit("open").as("priceType"), 'open.as("price"))
      .where('open > -0.1)

    val secPriceCloseDf = pricesSet
      .select('secId, 'exchangeId, 'date2.as("date"), lit("open").as("priceType"), 'close.as("price"))
      .where('close > -0.1)

    val secPriceBidDf = pricesSet
      .select('secId, 'exchangeId, 'date2.as("date"), lit("open").as("priceType"), 'bid.as("price"))
      .where('bid > -0.1)

    val secPriceOfferDf = pricesSet
      .select('secId, 'exchangeId, 'date2.as("date"), lit("open").as("priceType"), 'offer.as("price"))
      .where('offer > -0.1)

    val secPriceDf = secPriceOpenDf
      .union(secPriceCloseDf)
      .union(secPriceBidDf)
      .union(secPriceOfferDf)
      .coalesce(50)

    secPriceDf.write.mode(SaveMode.Overwrite).format(inputFormat).save(outputDirectoryName)
  }
}
