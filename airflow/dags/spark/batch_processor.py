from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql.types import *
from pyspark.sql import functions as func
from datetime import date, timedelta
import sys
sys.path.append("/opt/airflow/dags/spark/")
from schema import schema
import itertools

class Batch_Processor():
    def __init__(self) -> None:
        conf = SparkConf()
        conf.set("spark.cores.max", 1)
        conf.set("spark.executor.memory", "2g")
        conf.set("spark.driver.memory", "2g")
        self.spark = SparkSession.builder \
                    .master("spark://spark-master:7077") \
                    .config(conf=conf) \
                    .appName("BatchProcess") \
                    .getOrCreate()


        self.spark.sparkContext.setLogLevel("ERROR")
        self.spark.sparkContext.addPyFile("/opt/airflow/dags/spark/batch_processor.py")
        self.spark.sparkContext.addPyFile("/opt/airflow/dags/spark/schema.py")
        self.spark.sparkContext.addPyFile("/opt/airflow/dags/spark/utils.py")
        
    
    def path_exists(self, path):
        # spark is a SparkSession
        sc = self.spark.sparkContext
        fs = sc._jvm.org.apache.hadoop.fs.FileSystem.get(
                sc._jvm.java.net.URI.create(path),
                sc._jsc.hadoopConfiguration(),
        )
        return fs.exists(sc._jvm.org.apache.hadoop.fs.Path(path))
    
    def extract_data(self, topic, mode):
        if mode == "full":
            df = self.spark.read \
                    .format("parquet") \
                    .option("path", "hdfs://namenode:9000/data/{}".format(topic)) \
                    .load()
        elif mode == "incremental":
            date_param = date.today() - timedelta(days=1)
            path = "hdfs://namenode:9000/data/{}/date={}".format(topic, date_param)
            if self.path_exists(path):
                df = self.spark.read \
                        .format("parquet") \
                        .option("path", path) \
                        .load()
            else:
                df = self.spark.createDataFrame([], schema[topic])
        else:
            print('Mode "{}" does not support'.format(mode))
            df = self.spark.createDataFrame([], schema[topic])
            
        return df

    def extract_star_schema(self, table):
        df = self.spark.read \
                    .format("parquet") \
                    .option("path", "hdfs://namenode:9000/transformed_data/{}".format(table)) \
                    .load()
        return df

    def create_dim_time(self):
        time = [[x for x in range(24)], [x for x in range(60)], [x for x in range(60)]]
        combination = itertools.product(*time)
        df = self.spark.createDataframe(combination, ["hour", "minute", "second"]) \
            .withColumn("time_id", func.expr("uuid()"))
        return df
        
    def transform_listen_events(self, df):
        transformed_df = df.withColumn("hour", func.hour("ts")) \
            .withColumn("minute", func.minute("ts")) \
            .withColumn("second", func.second("ts")) \
            .withColumn("day", func.dayofmonth(func.col("ts"))) \
            .withColumn("dayOfWeek", func.dayofweek(func.col("ts"))) \
            .withColumn("week", func.weekofyear(func.col("ts"))) \
            .withColumn("month", func.month(func.col("ts"))) \
            .withColumn("year", func.year(func.col("ts"))) \
            .withColumn("quarter", func.quarter(func.col("ts")))
        
        dim_date = transformed_df.select("day", "dayOfWeek", "week", "month", "year", "quarter") \
            .dropDuplicates() \
            .na.drop(how="any") \
        
        dim_song = transformed_df.select(func.col("song").alias("title"), func.col("artist"), func.col("duration")) \
                .dropDuplicates() \
                .na.drop(how="any") \

        dim_user = transformed_df.select(func.col("userId"), func.col("lastName"), func.col("firstName"), func.col("gender"), func.col("level"), func.col("registration")) \
            .dropDuplicates() \
            .na.drop(how="any")
        
        dim_location = transformed_df.select(func.col("city"), func.col("zip"), func.col("state"), func.col("lon"), func.col("lat")) \
            .dropDuplicates() \
            .na.drop(how="any") \
            
        return {"listen_events_df": transformed_df,
                "dim_date": dim_date,
                "dim_song": dim_song,
                "dim_user": dim_user,
                "dim_location": dim_location
        }

    def transform_page_view_events(self, df):
        transformed_df = df.withColumn("hour", func.hour("ts")) \
            .withColumn("minute", func.minute("ts")) \
            .withColumn("second", func.second("ts")) \
            .withColumn("day", func.dayofmonth(func.col("ts"))) \
            .withColumn("dayOfWeek", func.dayofweek(func.col("ts"))) \
            .withColumn("week", func.weekofyear(func.col("ts"))) \
            .withColumn("month", func.month(func.col("ts"))) \
            .withColumn("year", func.year(func.col("ts"))) \
            .withColumn("quarter", func.quarter(func.col("ts")))
            
        
        dim_date = transformed_df.select("day", "dayOfWeek", "week", "month", "year", "quarter") \
            .dropDuplicates() \
            .na.drop(how="any") \
        
        dim_song = transformed_df.select(func.col("song").alias("title"), func.col("artist"), func.col("duration")) \
                .dropDuplicates() \
                .na.drop(how="any") \
        
        dim_user = transformed_df.select(func.col("userId"), func.col("lastName"), func.col("firstName"), func.col("gender"), func.col("level"), func.col("registration")) \
            .dropDuplicates() \
            .na.drop(how="any")
        
        dim_location = transformed_df.select(func.col("city"), func.col("zip"), func.col("state"), func.col("lon"), func.col("lat")) \
            .dropDuplicates() \
            .na.drop(how="any") \
            
        return {"page_view_events_df": transformed_df,
                "dim_date": dim_date,
                "dim_song": dim_song,
                "dim_user": dim_user,
                "dim_location": dim_location
        }

    def transform_auth_events(self, df):
        transformed_df = df.withColumn("hour", func.hour("ts")) \
            .withColumn("minute", func.minute("ts")) \
            .withColumn("second", func.second("ts")) \
            .withColumn("day", func.dayofmonth(func.col("ts"))) \
            .withColumn("dayOfWeek", func.dayofweek(func.col("ts"))) \
            .withColumn("week", func.weekofyear(func.col("ts"))) \
            .withColumn("month", func.month(func.col("ts"))) \
            .withColumn("year", func.year(func.col("ts"))) \
            .withColumn("quarter", func.quarter(func.col("ts")))
        
        dim_date = transformed_df.select("day", "dayOfWeek", "week", "month", "year", "quarter") \
            .dropDuplicates() \
            .na.drop(how="any") \
        
        dim_user = transformed_df.select(func.col("userId"), func.col("lastName"), func.col("firstName"), func.col("gender"), func.col("level"), func.col("registration")) \
            .dropDuplicates() \
            .na.drop(how="any")
        
        dim_location = transformed_df.select(func.col("city"), func.col("zip"), func.col("state"), func.col("lon"), func.col("lat")) \
            .dropDuplicates() \
            .na.drop(how="any") \
            
        return {"auth_events_df": transformed_df,
                "dim_user": dim_user,
                "dim_date": dim_date,
                "dim_location": dim_location}


    def load_to_hdfs(self, data, filename, mode):    
        stream = data.write \
            .format("parquet") \
            .mode(mode) \
            .option("path", "hdfs://namenode:9000/transformed_data/{}.parquet".format(filename)) \
            .save()
        return stream
    
    def stop(self):
        self.spark.stop()