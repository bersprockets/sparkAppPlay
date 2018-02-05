import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

object MaxTempsSql4 {
  case class Station(name: String, country: String)

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
      Console.err.println("No station file specified")
      System.exit(2)
    }
    val stationFilename = args(2)

    if (args.length < 4) {
      Console.err.println("No output file specified")
      System.exit(3)
    }
    val outputDirectory = args(3)

    val spark = SparkSession.builder().appName("MaxTempsSql4").getOrCreate()
    import spark.implicits._

    val stations = spark.read.textFile(stationFilename)
      .filter(s => s.length > 0 && s.charAt(0).isDigit)
      .map(stationLine => Station(stationLine.substring(0, 12), stationLine.substring(43, 45)))

    val inputSchema: StructType = StructType(
      StructField("stationName", StringType)
      :: StructField("year", IntegerType)
      :: StructField("month", IntegerType)
      :: StructField("day", IntegerType)
      :: StructField("temperature", DoubleType) :: Nil
    )
    val observationsWithoutCountry = spark.read.schema(inputSchema).format(inputFormat).load(inputFilename)

    val observations = observationsWithoutCountry.as("o")
      .join(stations.as("s"), $"o.stationName" === $"s.name")
      .select($"o.stationName", $"o.year", $"o.temperature", $"s.country")

    val maxTemps = observations.groupBy('year, 'country).max("temperature").coalesce(1)
    maxTemps.write.format("csv").save(outputDirectory)
  }
}
