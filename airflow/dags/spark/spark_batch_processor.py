from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql.types import *
from pyspark.sql import functions as func

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
        self.spark.sparkContext.addPyFile("/opt/airflow/dags/batch_processor.py")
    
    def extract_data(self, topic):
        df = self.spark.read \
                .format("parquet") \
                .option("path", "hdfs://namenode:9000/data/{}".format(topic)) \
                .load()
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
            
        dim_time = transformed_df.select("hour", "minute", "second") \
            .dropDuplicates() \
            .na.drop(how="any") \
        
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
            
        # fact_listen = df.join(dim_time, (df["hour"] == dim_time["hour"]) & (df["minute"] == dim_time["minute"]) & (df["second"] == dim_time["second"]), "inner") \
        #     .join(dim_date, (df["day"] == dim_date["day"]) & (df["dayOfWeek"] == dim_date["dayOfWeek"]) & (df["week"] == dim_date["week"]) & (df["month"] == dim_date["month"]) & (df["year"] == dim_date["year"]) & (df["quarter"] == dim_date["quarter"]), "inner") \
        #     .join(dim_song, (df["song"] == dim_song["title"]) & (df["artist"] == dim_song["artist"]) & (df["duration"] == dim_song["duration"]), "inner") \
        #     .join(dim_user, df["userId"] == dim_user["userId"], "inner").drop(df.userId) \
        #     .join(dim_location, (df["city"] == dim_location["city"]) & (df["zip"] == dim_location["zip"]) & (df["state"] == dim_location["state"]) & (df["lon"] == dim_location["lon"]) & (df["lat"] == dim_location["lat"]), "inner") \
        #     .select("time_id", "date_id", "song_id", func.col("userId"), "location_id", "sessionId", "itemInSession", "auth", "userAgent")
        
        return {"listen_events_df": transformed_df,
                "dim_time": dim_time,
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
            
        dim_time = transformed_df.select("hour", "minute", "second") \
            .dropDuplicates() \
            .na.drop(how="any") \
        
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
            
        # fact_page_view = df.join(dim_time, (df["hour"] == dim_time["hour"]) & (df["minute"] == dim_time["minute"]) & (df["second"] == dim_time["second"]), "inner") \
        #     .join(dim_date, (df["day"] == dim_date["day"]) & (df["dayOfWeek"] == dim_date["dayOfWeek"]) & (df["week"] == dim_date["week"]) & (df["month"] == dim_date["month"]) & (df["year"] == dim_date["year"]) & (df["quarter"] == dim_date["quarter"]), "inner") \
        #     .join(dim_song, (df["song"] == dim_song["title"]) & (df["artist"] == dim_song["artist"]) & (df["duration"] == dim_song["duration"]), "inner") \
        #     .join(dim_user, df["userId"] == dim_user["userId"], "inner").drop(df.userId) \
        #     .join(dim_location, (df["city"] == dim_location["city"]) & (df["zip"] == dim_location["zip"]) & (df["state"] == dim_location["state"]) & (df["lon"] == dim_location["lon"]) & (df["lat"] == dim_location["lat"]), "inner") \
        #     .select("time_id", "date_id", "song_id", 'userId', "location_id", "sessionId", "itemInSession", "page", "auth", "method", "status", "userAgent") \
        
        return {"page_view_events_df": transformed_df,
                "dim_time": dim_time,
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
            
        dim_time = transformed_df.select("hour", "minute", "second") \
            .dropDuplicates() \
            .na.drop(how="any") \
        
        dim_date = transformed_df.select("day", "dayOfWeek", "week", "month", "year", "quarter") \
            .dropDuplicates() \
            .na.drop(how="any") \
        
        dim_user = transformed_df.select(func.col("userId"), func.col("lastName"), func.col("firstName"), func.col("gender"), func.col("level"), func.col("registration")) \
            .dropDuplicates() \
            .na.drop(how="any")
        
        dim_location = transformed_df.select(func.col("city"), func.col("zip"), func.col("state"), func.col("lon"), func.col("lat")) \
            .dropDuplicates() \
            .na.drop(how="any") \
            
        # fact_auth = df.join(dim_time, (df["hour"] == dim_time["hour"]) & (df["minute"] == dim_time["minute"]) & (df["second"] == dim_time["second"]), "inner") \
        #     .join(dim_date, (df["day"] == dim_date["day"]) & (df["dayOfWeek"] == dim_date["dayOfWeek"]) & (df["week"] == dim_date["week"]) & (df["month"] == dim_date["month"]) & (df["year"] == dim_date["year"]) & (df["quarter"] == dim_date["quarter"]), "inner") \
        #     .join(dim_user, df["userId"] == dim_user["userId"], "inner").drop(df.userId) \
        #     .join(dim_location, (df["city"] == dim_location["city"]) & (df["zip"] == dim_location["zip"]) & (df["state"] == dim_location["state"]) & (df["lon"] == dim_location["lon"]) & (df["lat"] == dim_location["lat"]), "inner") \
        #     .select("time_id", "date_id", 'userId', "location_id", "sessionId", "itemInSession", "userAgent", "success") \
        
        return {"auth_events_df": transformed_df,
                "dim_user": dim_user,
                "dim_time": dim_time,
                "dim_date": dim_date,
                "dim_location": dim_location}


    def load_to_hdfs(self, data, filename):    
        stream = data.write \
            .format("parquet") \
            .mode("overwrite") \
            .option("path", "hdfs://namenode:9000/transformed_data/{}.parquet".format(filename)) \
            .save()
        return stream
    
    def stop(self):
        self.spark.stop()