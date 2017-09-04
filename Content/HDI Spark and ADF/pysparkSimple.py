from pyspark.sql import SparkSession


spark = SparkSession.builder \
     .appName("SparkonADF - Simple")\
     .enableHiveSupport()\
     .getOrCreate()

from pyspark.sql.functions import *

## Read in HVAC file(s)
df0 = spark.read.csv('wasb://#containername#@#storageaccount#.blob.core.windows.net/HdiSamples/HdiSamples/SensorSampleData/hvac', header = True, inferSchema = True)

## Get Avg Temp by BuildingID
df1 = df0.select(col('BuildingID'), col('ActualTemp')).groupBy('BuildingID').avg('ActualTemp')

## Write results to CSV file
df1.repartition(1).write.csv('wasb://#containername#@#storageaccount#.blob.core.windows.net/output/AvgTempByDay', header = True, mode = 'overwrite')
