from pyspark.sql import SparkSession
from schema import schema
from pyspark.sql import functions as func
from pyspark.sql.types import *
from pyspark import SparkConf



def fix_string(str):
        if str: 
            str = str.encode("latin-1") \
                .decode("unicode-escape") \
                .encode("latin-1") \
                .decode("utf-8") \
                .strip('\"')
        return str
    
class Streaming_Processor():
    def __init__(self) -> None:
        conf = SparkConf()
        conf.set("spark.cores.max", 2)
        conf.set("spark.executor.memory", "4g")
        conf.set("spark.driver.memory", "4g")
        conf.set("spark.cassandra.connection.host", "cassandra-1")
        conf.set("spark.cassandra.connection.port","9042")
        conf.set("spark.cassandra.auth.username", "cassandra")
        conf.set("spark.cassandra.auth.password", "cassandra")
    
        self.spark = SparkSession.builder \
            .master("spark://spark-master:7077") \
            .config(conf=conf) \
            .appName("KafkaConsumer").getOrCreate()
            
        self.spark.sparkContext.setLogLevel("ERROR")
        self.spark.sparkContext.addPyFile("/src/streaming_processor.py")
        self.spark.sparkContext.addPyFile("/src/schema.py")
        
    def extract_from_kafka(self, topic):
        data = self.spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", topic) \
        .option("startingOffsets", "latest") \
        .load()
        return data

    
    

    def transform_data(self, data, topic):
        
        convertudf = func.udf(lambda x: fix_string(x), StringType())
        
        transformed_data = data.selectExpr("CAST(value as String)") \
            .select(func.from_json(func.col("value"), schema[topic]).alias("value")) \
            .select(func.col("value.*")) \
            .withColumn("ts", (func.col("ts")/1000).cast("timestamp")) \
            .withColumn("registration", (func.col("registration")/1000).cast("timestamp"))
        
        if topic == "listen_events" or topic == "page_view_events":
            transformed_data = transformed_data.withColumn("song", convertudf(func.col("song"))) \
                                                .withColumn("artist", convertudf(func.col("artist")))
        
        return transformed_data

    def transform_song_chart(self, data, topic):
        convertudf = func.udf(lambda x: fix_string(x), StringType())
            
        transformed_data = data.selectExpr("CAST(value as String)") \
            .select(func.from_json(func.col("value"), schema[topic]).alias("value")) \
            .select(func.col("value.*")) \
            .withColumn("ts", (func.col("ts")/1000).cast("timestamp")) \
            .withWatermark("ts", "0 seconds") \
            .groupBy("song", func.window("ts", "1 minute", "1 minute")) \
            .count() \
            .withColumn("ingest_ts", func.col("window.start")) \
            .withColumn("year", func.year(func.col("ingest_ts"))) \
            .withColumn("month", func.month(func.col("ingest_ts"))) \
            .withColumn("week", func.weekofyear(func.col("ingest_ts"))) \
            .withColumn("day", func.dayofmonth(func.col("ingest_ts"))) \
            .withColumn("hour", func.hour(func.col("ingest_ts"))) \
            .withColumn("minute", func.minute(func.col("ingest_ts"))) \
            .select("song", "year", "month", "week", "day", "hour", "minute", "ingest_ts", "count") 
            
        transformed_data = transformed_data.withColumn("song", convertudf(func.col("song")))
            
        return transformed_data

    def transform_artist_chart(self, data, topic="listen_events"):
        convertudf = func.udf(lambda x: fix_string(x), StringType())
            
        transformed_data = data.selectExpr("CAST(value as String)") \
            .select(func.from_json(func.col("value"), schema[topic]).alias("value")) \
            .select(func.col("value.*")) \
            .withColumn("ts", (func.col("ts")/1000).cast("timestamp")) \
            .withWatermark("ts", "0 seconds") \
            .groupBy("artist", func.window("ts", "1 minute", "1 minute")) \
            .count() \
            .withColumn("ingest_ts", func.col("window.start")) \
            .withColumn("year", func.year(func.col("ingest_ts"))) \
            .withColumn("month", func.month(func.col("ingest_ts"))) \
            .withColumn("week", func.weekofyear(func.col("ingest_ts"))) \
            .withColumn("day", func.dayofmonth(func.col("ingest_ts"))) \
            .withColumn("hour", func.hour(func.col("ingest_ts"))) \
            .withColumn("minute", func.minute(func.col("ingest_ts"))) \
            .select("artist", "year", "week", "month", "day", "hour", "minute", "ingest_ts", "count") 
        
        transformed_data = transformed_data.withColumn("artist", convertudf(func.col("artist")))
        return transformed_data


    def transform_listen_count(self, data, topic="listen_events"):       
        transformed_data = data.selectExpr("CAST(value as String)") \
            .select(func.from_json(func.col("value"), schema[topic]).alias("value")) \
            .select(func.col("value.*")) \
            .withColumn("ts", (func.col("ts")/1000).cast("timestamp")) \
            .withWatermark("ts", "0 seconds") \
            .groupBy(func.window("ts", "1 minute", "1 minute")) \
            .count() \
            .withColumn("ingest_ts", func.col("window.start")) \
            .withColumn("year", func.year(func.col("ingest_ts"))) \
            .withColumn("month", func.month(func.col("ingest_ts"))) \
            .withColumn("week", func.weekofyear(func.col("ingest_ts"))) \
            .withColumn("day", func.dayofmonth(func.col("ingest_ts"))) \
            .withColumn("hour", func.hour(func.col("ingest_ts"))) \
            .withColumn("minute", func.minute(func.col("ingest_ts"))) \
            .select("year", "week", "month", "day", "hour", "minute", "ingest_ts", "count") 
    
        return transformed_data

    def transform_user_activity_count(self, data, topic="page_view_events"):
        
        transformed_data = data.selectExpr("CAST(value as String)") \
            .select(func.from_json(func.col("value"), schema[topic]).alias("value")) \
            .select(func.col("value.*")) \
            .withColumn("ts", (func.col("ts")/1000).cast("timestamp")) \
            .withWatermark("ts", "0 seconds") \
            .groupBy("state", func.window("ts", "1 minute", "1 minute")) \
            .count() \
            .withColumn("ingest_ts", func.col("window.start")) \
            .withColumn("year", func.year(func.col("ingest_ts"))) \
            .withColumn("month", func.month(func.col("ingest_ts"))) \
            .withColumn("week", func.weekofyear(func.col("ingest_ts"))) \
            .withColumn("day", func.dayofmonth(func.col("ingest_ts"))) \
            .withColumn("hour", func.hour(func.col("ingest_ts"))) \
            .withColumn("minute", func.minute(func.col("ingest_ts"))) \
            .withColumn("state", func.concat(func.lit("US-"), func.col("state"))) \
            .select("state", "year", "week", "month", "day", "hour", "minute", "ingest_ts", "count") 
    
        return transformed_data

    def transform_gender_distribution_minute(self, data, topic="auth_events"):
        transformed_data = data.selectExpr("CAST(value as String)") \
            .select(func.from_json(func.col("value"), schema[topic]).alias("value")) \
            .select(func.col("value.*")) \
            .withColumn("ts", (func.col("ts")/1000).cast("timestamp")) \
            .withWatermark("ts", "0 seconds") \
            .groupBy("gender", "level", func.window("ts", "1 minute", "1 minute")) \
            .count() \
            .withColumn("ingest_ts", func.col("window.start")) \
            .select("gender", "level", "count", "ingest_ts") \
            .na.drop(subset=["gender", "level"])
            
        return transformed_data

    def transform_user_login(self, data, topic="auth_events"):
        transformed_data = data.selectExpr("CAST(value as String)") \
            .select(func.from_json(func.col("value"), schema[topic]).alias("value")) \
            .select(func.col("value.*")) \
            .withColumn("ts", (func.col("ts")/1000).cast("timestamp")) \
            .withColumn("registration", (func.col("registration")/1000).cast("timestamp")) \
            .select("userId", "lastName", "firstName", "gender", "level", "registration", "ts") \
            .na.drop(subset=["userId"])
            
        return transformed_data
    
    def load_to_hdfs(self, data, topic, processingTime):    
        stream = data.withColumn("date", func.to_date(func.col("ts"))) \
            .writeStream \
            .format("parquet") \
            .partitionBy("date") \
            .outputMode("append") \
            .option("path", "hdfs://namenode:9000/data/{}".format(topic)) \
            .option("checkpointLocation", "hdfs://namenode:9000/checkpoint/{}".format(topic)) \
            .trigger(processingTime=processingTime)
        return stream
        
        
    def load_to_cassandra(self, data, table, processingTime):
        stream = data.writeStream \
            .format("org.apache.spark.sql.cassandra") \
            .option("checkpointLocation", "/tmp/checkpoint/{}".format(table)) \
            .options(table=table, keyspace="music_streaming") \
            .trigger(processingTime=processingTime) \
            .outputMode("append") \
            
        return stream

    # For debugging    
    def output_to_console(self, data, processingTime):
                
        stream = data.writeStream \
            .format("console") \
            .outputMode("append") \
            .option("truncate", False) \
            .trigger(processingTime=processingTime)
        
        return stream
    
    def stop(self):
        self.spark.streams.awaitAnyTermination()

        self.spark.stop()