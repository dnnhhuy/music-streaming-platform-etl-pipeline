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
    

def readstream_from_kafka(spark, topic):
    data = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", topic) \
        .option("startingOffsets", "latest") \
        .load()
    return data

def transform_data(data, topic):
    
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

def transform_song_chart(data, topic):
    convertudf = func.udf(lambda x: fix_string(x), StringType())
        
    transformed_data = data.selectExpr("CAST(value as String)") \
        .select(func.from_json(func.col("value"), schema[topic]).alias("value")) \
        .select(func.col("value.*")) \
        .withColumn("ts", (func.col("ts")/1000).cast("timestamp")) \
        .withWatermark("ts", "0 seconds") \
        .groupBy("song", func.window("ts", "1 minute", "1 minute")) \
        .count() \
        .withColumn("ingest_ts", func.from_utc_timestamp(func.current_timestamp(), "+07:00")) \
        .withColumn("year", func.year(func.col("ingest_ts"))) \
        .withColumn("month", func.month(func.col("ingest_ts"))) \
        .withColumn("week", func.weekofyear(func.col("ingest_ts"))) \
        .withColumn("day", func.dayofmonth(func.col("ingest_ts"))) \
        .withColumn("hour", func.hour(func.col("ingest_ts"))) \
        .withColumn("minute", func.minute(func.col("ingest_ts"))) \
        .select("song", "year", "month", "week", "day", "hour", "minute", "ingest_ts", "count") 
        
    transformed_data = transformed_data.withColumn("song", convertudf(func.col("song")))
        
    return transformed_data

def transform_artist_chart(data, topic="listen_events"):
    convertudf = func.udf(lambda x: fix_string(x), StringType())
        
    transformed_data = data.selectExpr("CAST(value as String)") \
        .select(func.from_json(func.col("value"), schema[topic]).alias("value")) \
        .select(func.col("value.*")) \
        .withColumn("ts", (func.col("ts")/1000).cast("timestamp")) \
        .withWatermark("ts", "0 seconds") \
        .groupBy("artist", func.window("ts", "1 minute", "1 minute")) \
        .count() \
        .withColumn("ingest_ts", func.from_utc_timestamp(func.current_timestamp(), "+07:00")) \
        .withColumn("year", func.year(func.col("ingest_ts"))) \
        .withColumn("month", func.month(func.col("ingest_ts"))) \
        .withColumn("week", func.weekofyear(func.col("ingest_ts"))) \
        .withColumn("day", func.dayofmonth(func.col("ingest_ts"))) \
        .withColumn("hour", func.hour(func.col("ingest_ts"))) \
        .withColumn("minute", func.minute(func.col("ingest_ts"))) \
        .select("artist", "year", "week", "month", "day", "hour", "minute", "ingest_ts", "count") 
    
    transformed_data = transformed_data.withColumn("artist", convertudf(func.col("artist")))
    return transformed_data


def transform_listen_count(data, topic="listen_events"):       
    transformed_data = data.selectExpr("CAST(value as String)") \
        .select(func.from_json(func.col("value"), schema[topic]).alias("value")) \
        .select(func.col("value.*")) \
        .withColumn("ts", (func.col("ts")/1000).cast("timestamp")) \
        .withWatermark("ts", "0 seconds") \
        .groupBy(func.window("ts", "1 minute", "1 minute")) \
        .count() \
        .withColumn("ingest_ts", func.from_utc_timestamp(func.current_timestamp(), "+07:00")) \
        .withColumn("year", func.year(func.col("ingest_ts"))) \
        .withColumn("month", func.month(func.col("ingest_ts"))) \
        .withColumn("week", func.weekofyear(func.col("ingest_ts"))) \
        .withColumn("day", func.dayofmonth(func.col("ingest_ts"))) \
        .withColumn("hour", func.hour(func.col("ingest_ts"))) \
        .withColumn("minute", func.minute(func.col("ingest_ts"))) \
        .select("year", "week", "month", "day", "hour", "minute", "ingest_ts", "count") 
   
    return transformed_data

def transform_user_activity_count(data, topic="page_view_events"):
    
    transformed_data = data.selectExpr("CAST(value as String)") \
        .select(func.from_json(func.col("value"), schema[topic]).alias("value")) \
        .select(func.col("value.*")) \
        .withColumn("ts", (func.col("ts")/1000).cast("timestamp")) \
        .withWatermark("ts", "0 seconds") \
        .groupBy("state", func.window("ts", "1 minute", "1 minute")) \
        .count() \
        .withColumn("ingest_ts", func.from_utc_timestamp(func.current_timestamp(), "+07:00")) \
        .withColumn("year", func.year(func.col("ingest_ts"))) \
        .withColumn("month", func.month(func.col("ingest_ts"))) \
        .withColumn("week", func.weekofyear(func.col("ingest_ts"))) \
        .withColumn("day", func.dayofmonth(func.col("ingest_ts"))) \
        .withColumn("hour", func.hour(func.col("ingest_ts"))) \
        .withColumn("minute", func.minute(func.col("ingest_ts"))) \
        .withColumn("state", func.concat(func.lit("US-"), func.col("state"))) \
        .select("state", "year", "week", "month", "day", "hour", "minute", "ingest_ts", "count") 
   
    return transformed_data

def transform_user_data(data, topic="auth_events"):
    transformed_data = data.selectExpr("CAST(value as String)") \
        .select(func.from_json(func.col("value"), schema[topic]).alias("value")) \
        .select(func.col("value.*")) \
        .withColumn("registration", (func.col("registration")/1000).cast("timestamp")) \
        .select("userId", "lastName", "firstName", "gender", "level", "registration") \
        .dropna(how="any")
    
    return transformed_data

def write_to_hdfs(data, topic, processingTime):    
    stream = data.writeStream \
        .format("parquet") \
        .outputMode("append") \
        .option("path", "hdfs://namenode:9000/data/{}".format(topic)) \
        .option("checkpointLocation", "hdfs://namenode:9000/checkpoint/{}".format(topic)) \
        .trigger(processingTime=processingTime)
    return stream
        

    
def write_to_cassandra(data, table, processingTime):
    stream = data.writeStream \
        .format("org.apache.spark.sql.cassandra") \
        .option("checkpointLocation", "/tmp/chechpint/{}".format(table)) \
        .options(table=table, keyspace="music_streaming") \
        .trigger(processingTime=processingTime) \
        .outputMode("append") \
        
    return stream

    
def write_to_console(data, processingTime):
            
    stream = data.writeStream \
        .format("console") \
        .outputMode("append") \
        .option("truncate", False) \
        .trigger(processingTime=processingTime)
    
    return stream 


def initialize_spark():
    conf = SparkConf()
    conf.set("spark.cores.max", 2)
    conf.set("spark.executor.memory", "4g")
    conf.set("spark.driver.memory", "4g")
    conf.set("spark.cassandra.connection.host", "cassandra-1")
    conf.set("spark.cassandra.connection.port","9042")
    conf.set("spark.cassandra.auth.username", "cassandra")
    conf.set("spark.cassandra.auth.password", "cassandra")
    
    spark = SparkSession.builder \
        .master("spark://spark-master:7077") \
        .config(conf=conf) \
        .appName("KafkaConsumer").getOrCreate()
        
    return spark


if __name__ == '__main__':
    
    # Initialize Spark
    spark = initialize_spark()
    
    spark.sparkContext.setLogLevel("ERROR")
    
    # Process Stream Listen Events
    listen_events = readstream_from_kafka(spark, "listen_events")
    listen_events_hdfs = transform_data(listen_events, "listen_events")
    listen_events_hdfs = write_to_hdfs(listen_events_hdfs, "listen_events", "1 minute")
    
    listen_count_minute = transform_listen_count(listen_events, "listen_events")
    listen_count_minute = write_to_cassandra(listen_count_minute, "listen_count_minute", "1 minute")

    song_chart_minute = transform_song_chart(listen_events, "listen_events")
    song_chart_minute = write_to_cassandra(song_chart_minute, "song_chart_minute", "1 minute")

    artist_chart_minute = transform_artist_chart(listen_events, "listen_events")
    artist_chart_minute = write_to_cassandra(artist_chart_minute, "artist_chart_minute", "1 minute")
    
    
    
    # Process Stream Auth Events
    auth_events = readstream_from_kafka(spark, "auth_events")
    auth_events_hdfs = transform_data(auth_events, "auth_events")
    auth_events_hdfs = write_to_hdfs(auth_events_hdfs, "auth_events", "1 minute")

    user_auth = transform_user_data(auth_events, topic="auth_events")
    user_auth = write_to_cassandra(user_auth, "user_auth", "1 minute")

    # Process Stream Page View Events
    page_view_events = readstream_from_kafka(spark, "page_view_events")
    page_view_events_hdfs = transform_data(page_view_events, "page_view_events")
    page_view_events_hdfs = write_to_hdfs(page_view_events_hdfs, "page_view_events", "1 minute")
   
    user_activity_minute = transform_user_activity_count(page_view_events, "page_view_events")
    user_activity_minute = write_to_cassandra(user_activity_minute, "user_activity_minute", "1 minute")
    
    
    listen_count_minute.start()
    song_chart_minute.start()
    artist_chart_minute.start()
    
    user_activity_minute.start()
    user_auth.start()
    
    spark.streams.awaitAnyTermination()

    spark.stop()

    
