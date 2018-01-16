import org.apache.spark.sql.SparkSession

object MaxTempsSql1 {
  case class Station(name: String, country: String)
  case class Observation(stationName: String, year: Int, temperature: Float, country: String)

  def main(args: Array[String]) {
    if (args.length < 1) {
      Console.err.println("No input file specified");
      System.exit(1)
    }
    val inputFilename = args(0)

    if (args.length < 2) {
      Console.err.println("No station file specified");
      System.exit(2)
    }
    val stationFilename = args(1)

    if (args.length < 3) {
      Console.err.println("No output file specified");
      System.exit(3)
    }
    val outputDirectory = args(2)

    val spark = SparkSession.builder().appName("MaxTempsSql").getOrCreate()

    import spark.implicits._

    val stationsTextFile = spark.sparkContext.textFile(stationFilename)
      .filter(x => x.length > 0 && x.charAt(0).isDigit)

    val stationsDF = stationsTextFile
      .map(line => (line.substring(0, 12), line.substring(43, 45)))
      .map(attributes => Station(attributes._1, attributes._2))
      .toDF()

    stationsDF.createOrReplaceTempView("stations")

    val stationYearTempsTextFile = spark.sparkContext.textFile(inputFilename).filter(!_.startsWith("STN---"))
    val stationYearTempsDF = stationYearTempsTextFile
      .map(x => (x.substring(0, 12), x.substring(14, 18).toInt, x.substring(24, 30).toFloat))
      .map(attributes => Observation(attributes._1, attributes._2, attributes._3, ""))
      .toDF()

    stationYearTempsDF.createOrReplaceTempView("stationYearTemps")

    val observationsDF = spark.sql("select stationName, year, temperature, s.country " +
      "from stationYearTemps o, stations s " +
      "where o.stationName = s.name")

    observationsDF.createOrReplaceTempView("observations")

    val maxTempsDF = spark.sql("select year, country, max(temperature) maxTemp " +
      "from observations group by year, country")

    maxTempsDF.write.format("csv").save(outputDirectory)
    // maxTempsDF.write.partitionBy("year", "country").option("path", outputDirectory).saveAsTable("maxTemps")
  }
}
