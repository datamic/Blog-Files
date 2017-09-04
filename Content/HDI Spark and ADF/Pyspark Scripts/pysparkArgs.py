from pyspark.sql import SparkSession

spark = SparkSession.builder \
     .appName("SparkonADF - with Args")\
     .enableHiveSupport()\
     .getOrCreate()

from pyspark.sql.functions import *
import sys #Necessary for arguments


# Read in HVAC file(s)
df0 = spark.read.csv('wasb://#containername#@#storageaccount#.blob.core.windows.net/HdiSamples/HdiSamples/SensorSampleData/hvac', header = True, inferSchema = True)

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


#Write results to CSV
df1.repartition(1).write.csv('wasb://#containername#@#storageaccount#.blob.core.windows.net/output/AvgTempBuildingDay/Date=' + sys.argv[1], header = True, mode = 'overwrite')
