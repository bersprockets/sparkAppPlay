import sys
import pyspark.sql.session as session
import string
from pyspark.sql.types import *
from pyspark.sql.functions import substring

format = sys.argv[1]
input_filename = sys.argv[2]
station_filename = sys.argv[3]
output_filename = sys.argv[4]

spark = session.SparkSession \
               .builder \
               .appName("maxTempsSql4") \
               .getOrCreate()

stations_raw = spark.read \
                    .text(station_filename) \
                    .where("length(value) > 0 and substring(value, 1 ,1) >= '0' and substring(value, 1, 1) <= '9'")

stations = stations_raw.select(substring(stations_raw.value, 1, 12).alias("stationName"), \
                               substring(stations_raw.value, 44, 2).alias("country"))
input_fields = [StructField("stationName", StringType()), StructField("year", IntegerType()),\
                StructField("month", IntegerType()), StructField("day", IntegerType()),\
                StructField("temperature", DoubleType())]
input_schema = StructType(input_fields)

observations_without_country = spark.read.format(format).schema(input_schema).load(input_filename)

observations = observations_without_country.\
               join(stations, observations_without_country.stationName == stations.stationName)

max_temps = observations.groupBy("year", "country").max("temperature").coalesce(1)

max_temps.write.format("csv").save(output_filename)

