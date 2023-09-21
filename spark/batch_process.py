from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql import functions as func

spark = SparkSession.builder \
                    .master("spark://spark-master:7077") \
                    .config("spark.cores.max", 2) \
                    .config("spark.executor.memory", "4g") \
                    .config("spark.driver.memory", "4g") \
                    .appName("BatchProcess") \
                    .getOrCreate()
                    

listen_events_df = spark.read \
                    .format("parquet") \
                    .load("hdfs://namenode:8020/data/listen_events")
                     

listen_events_df = listen_events_df.groupBy("song").count().sort(func.desc(func.col("count")))

listen_events_df.show()

spark.stop()