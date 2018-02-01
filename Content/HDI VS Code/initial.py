from pyspark.sql import SparkSession
from pyspark.sql import Window
from pyspark.sql.functions import *
import sys 

spark = SparkSession.builder \
     .appName("zonarOperatingHours-Daily")\
     .enableHiveSupport()\
     .getOrCreate()

spark.sql('MSCK REPAIR TABLE zon_gps_incr_raw')