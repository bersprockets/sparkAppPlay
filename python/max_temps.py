import sys
import pyspark.context as context
import string

input_filename = sys.argv[1]
station_filename = sys.argv[2]
output_filename = sys.argv[3]

sc = context.SparkContext(appName="MaxTemps")

station_file = sc.textFile(station_filename).filter(lambda x: len(x) > 0 and x[0:1] in string.digits)
stations = station_file.map(lambda x: (x[0:12], x[43:45]))

observations_without_country = sc.textFile(input_filename) \
                                 .filter(lambda x: x.find("STN---") == -1) \
                                 .map(lambda x: (x[0:12], (int(x[14:18]), float(x[24:30]))))


observations_with_country = observations_without_country \
                            .join(stations) \
                            .map(lambda (station, ((year, temperature), country)): \
                                 ((year, country), temperature))

results = observations_with_country.reduceByKey(lambda x, y: max(x, y)).coalesce(1)
results = results.sortByKey()
results.map(lambda ((year, country), temperature): \
            "%d,%s,%0.1f" % (year, country.strip(), temperature)) \
       .saveAsTextFile(output_filename)
