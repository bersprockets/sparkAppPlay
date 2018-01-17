import org.apache.spark.sql.SparkSession

object MaxTempsSql2 {
  case class Station(name: String, country: String)
  case class Observation(stationName: String, year: Int, temperature: Float, country: String)

  def main(args: Array[String]) {
    if (args.length < 1) {
      Console.err.println("No input file specified")
      System.exit(1)
    }
    val inputFilename = args(0)

    if (args.length < 2) {
      Console.err.println("No station file specified")
      System.exit(2)
    }
    val stationFilename = args(1)

    if (args.length < 3) {
      Console.err.println("No output file specified")
      System.exit(3)
    }
    val outputDirectory = args(2)

    val spark = SparkSession.builder().appName("MaxTempsSql").getOrCreate()
    import spark.implicits._

    val stations = spark.read.textFile(stationFilename)
      .filter(s => s.length > 0 && s.charAt(0).isDigit)
      .map(stationLine => Station(stationLine.substring(0, 12), stationLine.substring(43, 45)))

    val observationsWithoutCountry = spark.read.text(inputFilename)
      .filter(!$"value".startsWith("STN---"))
      .map(line => {
        val lineString = line.getString(0)
        val stationName = lineString.substring(0, 12)
        val year = lineString.substring(14, 18).toInt
        val temperature = lineString.substring(24, 30).toFloat
        Observation(stationName, year, temperature, "")
      })

    val observations = observationsWithoutCountry.as("o")
      .join(stations.as("s"), $"o.stationName" === $"s.name")
      .select($"o.stationName", $"o.year", $"o.temperature", $"s.country")

    val maxTemps = observations.groupBy($"year", $"country").max("temperature").coalesce(1)
    maxTemps.write.format("csv").save(outputDirectory)
  }
}
