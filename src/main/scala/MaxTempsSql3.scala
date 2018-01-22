import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

object MaxTempsSql3 {
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

    val inputSchema: StructType = StructType(
      StructField("year", IntegerType)
      :: StructField("month", IntegerType)
      :: StructField("day", IntegerType)
      :: StructField("stationName", StringType)
      :: StructField("temperature", DoubleType) :: Nil
    )
    val observationsWithoutCountry = spark.read.schema(inputSchema).json(inputFilename)

    val observations = observationsWithoutCountry.as("o")
      .join(stations.as("s"), $"o.stationName" === $"s.name")
      .select($"o.stationName", $"o.year", $"o.temperature", $"s.country")

    val maxTemps = observations.groupBy('year, 'country).max("temperature").coalesce(1)
    maxTemps.write.format("csv").save(outputDirectory)
  }
}
