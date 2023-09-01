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
        .option("startingOffsets", "earliest") \
        .load()
    return data

def transform_data(data, topic):
    
    convertudf = func.udf(lambda x: fix_string(x), StringType())
    
    transformed_data = data.selectExpr("CAST(value as String)") \
        .select(func.from_json(func.col("value"), schema[topic]).alias("value")) \
        .select(func.col("value.*")) \
        .withColumn("ts", (func.col("ts")/1000).cast("timestamp")) \
        .withColumn("registration", (func.col("registration")/1000).cast("timestamp")) \
        .withColumn("day", func.dayofmonth(func.col("ts"))) \
        .withColumn("month", func.month(func.col("ts"))) \
        .withColumn("year", func.year(func.col("ts"))) \
        .withColumn("hour", func.hour(func.col("ts"))) \
        .withColumn("week", func.weekofyear(func.col("ts")))
    
    if topic == "listen_events" or topic == "page_view_events":
        transformed_data = transformed_data.withColumn("song", convertudf(func.col("song"))) \
                                            .withColumn("artist", convertudf(func.col("artist")))
    
    return transformed_data


def write_to_hdfs(data, topic):    
    stream = data.writeStream \
        .format("parquet") \
        .outputMode("append") \
        .option("path", "hdfs://namenode:8020/data/{}".format(topic)) \
        .option("checkpointLocation", "hdfs://namenode:8020/checkpoint/{}".format(topic)) \
        .trigger(processingTime="1 minutes")
    return stream
        


def write_to_cassandra(data, topic):
    stream = data.writeStream \
        .format("org.apache.spark.sql.cassandra") \
        .outputMode("append") \
        .options(table=topic, keyspace="music_streaming") \
        .option("checkpointLocation", "/tmp/checkpoint/{}".format(topic)) \
        
    return stream

def initialize_spark():
    conf = SparkConf()
    conf.set("spark.cores.max", 2)
    conf.set("spark.executor.memory", "4g")
    conf.set("spark.driver.memory", "4g")
    conf.set("spark.cassandra.connection.host", "cassandra")
    conf.set("spark.cassandra.connection.port","9042")
    # conf.set("spark.cassandra.auth.username", "cassandra")
    # conf.set("spark.cassandra.auth.password", "cassandra")
    
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
    listen_events = transform_data(listen_events, "listen_events")
    listen_events_cassandra = write_to_cassandra(listen_events, "listen_events")
    listen_events_hdfs = write_to_hdfs(listen_events, "listen_events")
    
    # Process Stream Auth Events
    auth_events = readstream_from_kafka(spark, "auth_events")
    auth_events = transform_data(auth_events, "auth_events")
    auth_events_cassandra = write_to_cassandra(auth_events, "auth_events")
    auth_events_hdfs = write_to_hdfs(auth_events, "auth_events")
    
    # Process Stream Page View Events
    page_view_events = readstream_from_kafka(spark, "page_view_events")
    page_view_events = transform_data(page_view_events, "page_view_events")
    page_view_events_cassandra = write_to_cassandra(page_view_events, "page_view_events")
    page_view_events_hdfs = write_to_hdfs(page_view_events, "page_view_events")
    
    listen_events_cassandra.start()
    # listen_events_hdfs.start()
    
    auth_events_cassandra.start()
    # auth_events_hdfs.start()
    
    page_view_events_cassandra.start()
    # page_view_events_hdfs.start()
    
    spark.streams.awaitAnyTermination()

    spark.stop()

    
