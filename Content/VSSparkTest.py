from pyspark.sql import SparkSession

spark = SparkSession.builder \
     .master("local")\
     .appName("VS Test")\
     .enableHiveSupport()\
     .getOrCreate()

from pyspark.sql.functions import *

df0 = spark.read.json("adl://insightsproddls.azuredatalakestore.net/raw/zonar/gps/incremental/transferdate=20170709")
df1 = spark.read.json("adl://insightsproddls.azuredatalakestore.net/raw/zonar/gps/incremental/transferdate=20170710")
df2 = spark.read.json("adl://insightsproddls.azuredatalakestore.net/raw/zonar/gps/incremental/transferdate=20170711")

df = df0.union(df1).union(df2)

dfwrite = df.groupBy(col("account.code"), date_format(col("insert_ts"), 'yyyy-MM-dd').alias("date")).count().orderBy(col("count").desc()).filter(col("date") == "2017-07-10")
dfwrite.repartition(1).write.csv("adl://insightsproddls.azuredatalakestore.net/serve/Mike/recon", header = True)
