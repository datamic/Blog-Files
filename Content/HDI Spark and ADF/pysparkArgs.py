from pyspark.sql import SparkSession

spark = SparkSession.builder \
     .master("local")\
     .appName("SparkonADF")\
     .enableHiveSupport()\
     .getOrCreate()

from pyspark.sql.functions import *
import sys


# Read in HVAC file(s)
df0 = spark.read.csv('wasb://theflashr-hdi@herostore001.blob.core.windows.net/HdiSamples/HdiSamples/SensorSampleData/hvac', header = True, inferSchema = True)

# Get avg temp by date and buildingID filtered by arg1
df1 = df0.select(
            date_format(
                from_unixtime(
                    unix_timestamp(
                        concat_ws(' ', col('Date'), col('Time')), 'M/dd/yy h:mm:ss')), 'yyyy-MM-dd').alias('Date'),
            col('BuildingId'), \
            col('ActualTemp')) \
        .groupBy('Date', 'BuildingId') \
        .avg('ActualTemp') \
        .filter(col('Date') == sys.argv[1]) \
        .show()

#Write results to CSV
df2 = df1.repartition(1).write.csv('wasb://theflashr-hdi@herostore001.blob.core.windows.net/output/AvgTempBuildingDay/' + sys.argv[1], header = True, mode = 'overwrite')
