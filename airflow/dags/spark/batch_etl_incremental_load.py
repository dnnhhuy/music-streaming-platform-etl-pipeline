import functools
from batch_processor import Batch_Processor
from pyspark.sql import functions as func

def unionAll(dfs):
    return functools.reduce(lambda df1, df2: df1.union(df2.select(df1.columns)), dfs)

def batch_etl(processor):
    # Extract data
    listen_events_df = processor.extract_data("listen_events").filter(func.to_date(func.col("ts")) == func.date_sub(func.current_date(), 1))
    auth_events_df = processor.extract_data("auth_events").filter(func.to_date(func.col("ts")) == func.date_sub(func.current_date(), 1))
    page_view_events_df = processor.extract_data("page_view_events").filter(func.to_date(func.col("ts")) == func.date_sub(func.current_date(), 1))
    
    
    # Transform data
    listen_events_df = processor.transform_listen_events(listen_events_df)
    auth_events_df = processor.transform_auth_events(auth_events_df)
    page_view_events_df = processor.transform_page_view_events(page_view_events_df) \
            .withColumn("registration", func.when(func.col("registration").isNotNull(), func.col("registration")).otherwise(func.lit("empty"))) \
            .na.fill(0.0, subset=["duration"]) \
            .withColumn("userId", func.col("userId") + 2) \
            .withColumn("userId", func.coalesce(func.col("userId"), func.when(func.col("level")=="free", 0).otherwise("1"))) \
            .na.fill("empty").cache()
    
    dim_time = unionAll([listen_events_df["dim_time"], auth_events_df["dim_time"], page_view_events_df["dim_time"]]) \
                .distinct() \
                .withColumn("time_id", func.expr("uuid()")) \
                .cache()
    dim_date = unionAll([listen_events_df["dim_date"], auth_events_df["dim_date"], page_view_events_df["dim_date"]]) \
                .distinct() \
                .withColumn("date_id", func.expr("uuid()")) \
                .cache()
    dim_user = unionAll([listen_events_df["dim_user"], auth_events_df["dim_user"], page_view_events_df["dim_user"]]) \
                .distinct() \
                .cache()
    dim_location = unionAll([listen_events_df["dim_location"], auth_events_df["dim_location"], page_view_events_df["dim_location"]]) \
                .distinct() \
                .withColumn("location_id", func.expr("uuid()")) \
                .cache()
    dim_song = unionAll([listen_events_df["dim_song"], page_view_events_df["dim_song"]]) \
                .distinct() \
                .withColumn("song_id", func.expr("uuid()")) \
                .cache()
    
    fact_listen = listen_events_df["listen_events_df"].join(dim_time, (listen_events_df["listen_events_df"]["hour"] == dim_time["hour"]) & (listen_events_df["listen_events_df"]["minute"] == dim_time["minute"]) & (listen_events_df["listen_events_df"]["second"] == dim_time["second"]), "inner") \
            .join(dim_date, (listen_events_df["listen_events_df"]["day"] == dim_date["day"]) & (listen_events_df["listen_events_df"]["dayOfWeek"] == dim_date["dayOfWeek"]) & (listen_events_df["listen_events_df"]["week"] == dim_date["week"]) & (listen_events_df["listen_events_df"]["month"] == dim_date["month"]) & (listen_events_df["listen_events_df"]["year"] == dim_date["year"]) & (listen_events_df["listen_events_df"]["quarter"] == dim_date["quarter"]), "inner") \
            .join(dim_song, (listen_events_df["listen_events_df"]["song"] == dim_song["title"]) & (listen_events_df["listen_events_df"]["artist"] == dim_song["artist"]) & (listen_events_df["listen_events_df"]["duration"] == dim_song["duration"]), "inner") \
            .join(dim_user, listen_events_df["listen_events_df"]["userId"] == dim_user["userId"], "inner").drop(listen_events_df["listen_events_df"].userId) \
            .join(dim_location, (listen_events_df["listen_events_df"]["city"] == dim_location["city"]) & (listen_events_df["listen_events_df"]["zip"] == dim_location["zip"]) & (listen_events_df["listen_events_df"]["state"] == dim_location["state"]) & (listen_events_df["listen_events_df"]["lon"] == dim_location["lon"]) & (listen_events_df["listen_events_df"]["lat"] == dim_location["lat"]), "inner") \
            .select("time_id", "date_id", "song_id", func.col("userId"), "location_id", "sessionId", "itemInSession", "auth", "userAgent")
    
    fact_auth = auth_events_df["auth_events_df"].join(dim_time, (auth_events_df["auth_events_df"]["hour"] == dim_time["hour"]) & (auth_events_df["auth_events_df"]["minute"] == dim_time["minute"]) & (auth_events_df["auth_events_df"]["second"] == dim_time["second"]), "inner") \
            .join(dim_date, (auth_events_df["auth_events_df"]["day"] == dim_date["day"]) & (auth_events_df["auth_events_df"]["dayOfWeek"] == dim_date["dayOfWeek"]) & (auth_events_df["auth_events_df"]["week"] == dim_date["week"]) & (auth_events_df["auth_events_df"]["month"] == dim_date["month"]) & (auth_events_df["auth_events_df"]["year"] == dim_date["year"]) & (auth_events_df["auth_events_df"]["quarter"] == dim_date["quarter"]), "inner") \
            .join(dim_user, auth_events_df["auth_events_df"]["userId"] == dim_user["userId"], "inner").drop(auth_events_df["auth_events_df"].userId) \
            .join(dim_location, (auth_events_df["auth_events_df"]["city"] == dim_location["city"]) & (auth_events_df["auth_events_df"]["zip"] == dim_location["zip"]) & (auth_events_df["auth_events_df"]["state"] == dim_location["state"]) & (auth_events_df["auth_events_df"]["lon"] == dim_location["lon"]) & (auth_events_df["auth_events_df"]["lat"] == dim_location["lat"]), "inner") \
            .select("time_id", "date_id", 'userId', "location_id", "sessionId", "itemInSession", "userAgent", "success")
    
    fact_page_view = page_view_events_df["page_view_events_df"].join(dim_time, (page_view_events_df["page_view_events_df"]["hour"] == dim_time["hour"]) & (page_view_events_df["page_view_events_df"]["minute"] == dim_time["minute"]) & (page_view_events_df["page_view_events_df"]["second"] == dim_time["second"]), "inner") \
            .join(dim_date, (page_view_events_df["page_view_events_df"]["day"] == dim_date["day"]) & (page_view_events_df["page_view_events_df"]["dayOfWeek"] == dim_date["dayOfWeek"]) & (page_view_events_df["page_view_events_df"]["week"] == dim_date["week"]) & (page_view_events_df["page_view_events_df"]["month"] == dim_date["month"]) & (page_view_events_df["page_view_events_df"]["year"] == dim_date["year"]) & (page_view_events_df["page_view_events_df"]["quarter"] == dim_date["quarter"]), "inner") \
            .join(dim_song, (page_view_events_df["page_view_events_df"]["song"] == dim_song["title"]) & (page_view_events_df["page_view_events_df"]["artist"] == dim_song["artist"]) & (page_view_events_df["page_view_events_df"]["duration"] == dim_song["duration"]), "inner") \
            .join(dim_user, page_view_events_df["page_view_events_df"]["userId"] == dim_user["userId"], "inner").drop(page_view_events_df["page_view_events_df"].userId) \
            .join(dim_location, (page_view_events_df["page_view_events_df"]["city"] == dim_location["city"]) & (page_view_events_df["page_view_events_df"]["zip"] == dim_location["zip"]) & (page_view_events_df["page_view_events_df"]["state"] == dim_location["state"]) & (page_view_events_df["page_view_events_df"]["lon"] == dim_location["lon"]) & (page_view_events_df["page_view_events_df"]["lat"] == dim_location["lat"]), "inner") \
            .select("time_id", "date_id", "song_id", 'userId', "location_id", "sessionId", "itemInSession", "page", "auth", "method", "status", "userAgent")
    
    # Load data to hdfs
    processor.load_to_hdfs(fact_listen, "fact_listen", "append")
    processor.load_to_hdfs(fact_auth, "fact_auth", "append")
    processor.load_to_hdfs(fact_page_view, "fact_page_view", "append")
    
    processor.load_to_hdfs(dim_date, "dim_date", "append")
    processor.load_to_hdfs(dim_location, "dim_location", "append")
    processor.load_to_hdfs(dim_song, "dim_song", "append")
    processor.load_to_hdfs(dim_time, "dim_time", "append")
    processor.load_to_hdfs(dim_user, "dim_user", "append")
    

if __name__ == '__main__':
    processor = Batch_Processor()
    batch_etl(processor)