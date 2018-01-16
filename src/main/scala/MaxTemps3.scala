/* MaxTemps.scala */

import org.apache.spark.{SparkConf, SparkContext}

object MaxTemps3 {
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
      Console.err.println("No output directory specified");
      System.exit(3)
    }
    val outputDirectory = args(2)

    val conf = new SparkConf().setAppName("Max Temps")
    val sc = new SparkContext(conf)

    // read in station file. Filter out header data
    val stationFile = sc.textFile(stationFilename).filter(x => x.length > 0 && x.charAt(0).isDigit)

    // create tuples (station, country)
    val stations = stationFile
      .map(line => {
        val station = line.substring(0, 12)
        val country = line.substring(43, 45)
        (station, country)
      })

    // read temperature observations. Filter out column headers
    val textFile = sc.textFile(inputFilename).filter(!_.startsWith("STN---"))

    // create tuples (station, (year, temperature))
    val stationYearTemp = textFile.map(x => (x.substring(0, 12), (x.substring(14, 18).toInt, x.substring(24, 30).toFloat)))

    // create tuples ((year, country), temperature)
    val yearCountryTemp = stationYearTemp.join(stations)
      .map { case (station, ((year, temperature), country)) =>
        ((year, country), temperature)
      }

    // reduce: get max temperature for each key (year, country)
    val results = yearCountryTemp.reduceByKey((x, y) => if (x > y) x else y)
    results.saveAsTextFile(outputDirectory)
  }
}