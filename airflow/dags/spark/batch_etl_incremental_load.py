from batch_processor import Batch_Processor
from pyspark.sql import functions as func
from utils import *

def batch_etl(processor):
    # Extract data
    listen_events_df = processor.extract_data("listen_events", "incremental")
    auth_events_df = processor.extract_data("auth_events", "incremental")
    page_view_events_df = processor.extract_data("page_view_events", "incremental") \
            .withColumn("registration", func.when(func.col("registration").isNotNull(), func.col("registration")).otherwise(func.lit("empty"))) \
            .na.fill(0.0, subset=["duration"]) \
            .withColumn("userId", func.coalesce(func.col("userId"), func.when(func.col("level")=="free", "0").otherwise("1"))) \
            .na.fill("empty").cache()
            
    dim_time = processor.extract_dimension("dim_time").cache()
    dim_date = processor.extract_dimension("dim_date")
    dim_user = processor.extract_dimension("dim_user")
    dim_location = processor.extract_dimension("dim_location")
    dim_song = processor.extract_dimension("dim_song")

    # Transform data
    listen_events_dict = processor.transform_listen_events(listen_events_df)
    auth_events_dict = processor.transform_auth_events(auth_events_df)
    page_view_events_dict = processor.transform_page_view_events(page_view_events_df)
    
    incremental_dim_date = unionAll([listen_events_dict["dim_date"], auth_events_dict["dim_date"], page_view_events_dict["dim_date"]]) \
                .subtract(dim_date.select("day", "dayOfWeek", "week", "month", "year", "quarter")) \
                .distinct() \
                .withColumn("date_id", func.expr("uuid()"))
    
    incremental_dim_user = unionAll([listen_events_dict["dim_user"], auth_events_dict["dim_user"], page_view_events_dict["dim_user"]]) \
                .subtract(dim_user) \
                .distinct()
                
    incremental_dim_location = unionAll([listen_events_dict["dim_location"], auth_events_dict["dim_location"], page_view_events_dict["dim_location"]]) \
                .subtract(dim_location.select("city", "zip", "state", "lon", "lat")) \
                .distinct() \
                .withColumn("location_id", func.expr("uuid()"))
                
    incremental_dim_song = unionAll([listen_events_dict["dim_song"], page_view_events_dict["dim_song"]]) \
                .subtract(dim_song.select("title", "artist", "duration")) \
                .distinct() \
                .withColumn("song_id", func.expr("uuid()"))
    
    dim_date = unionAll([dim_date, incremental_dim_date]).cache()
    dim_song = unionAll([dim_song, incremental_dim_song]).cache()
    dim_location = unionAll([dim_location, incremental_dim_location]).cache()
    dim_user = unionAll([dim_user, incremental_dim_user]).cache()
        
    incremental_fact_listen = listen_events_dict["listen_events_df"].join(dim_time, (listen_events_dict["listen_events_df"]["hour"] == dim_time["hour"]) & (listen_events_dict["listen_events_df"]["minute"] == dim_time["minute"]) & (listen_events_dict["listen_events_df"]["second"] == dim_time["second"]), "inner") \
            .join(dim_date, (listen_events_dict["listen_events_df"]["day"] == dim_date["day"]) & (listen_events_dict["listen_events_df"]["dayOfWeek"] == dim_date["dayOfWeek"]) & (listen_events_dict["listen_events_df"]["week"] == dim_date["week"]) & (listen_events_dict["listen_events_df"]["month"] == dim_date["month"]) & (listen_events_dict["listen_events_df"]["year"] == dim_date["year"]) & (listen_events_dict["listen_events_df"]["quarter"] == dim_date["quarter"]), "inner") \
            .join(dim_song, (listen_events_dict["listen_events_df"]["song"] == dim_song["title"]) & (listen_events_dict["listen_events_df"]["artist"] == dim_song["artist"]) & (listen_events_dict["listen_events_df"]["duration"] == dim_song["duration"]), "inner") \
            .join(dim_user, listen_events_dict["listen_events_df"]["userId"] == dim_user["userId"], "inner").drop(listen_events_dict["listen_events_df"].userId) \
            .join(dim_location, (listen_events_dict["listen_events_df"]["city"] == dim_location["city"]) & (listen_events_dict["listen_events_df"]["zip"] == dim_location["zip"]) & (listen_events_dict["listen_events_df"]["state"] == dim_location["state"]) & (listen_events_dict["listen_events_df"]["lon"] == dim_location["lon"]) & (listen_events_dict["listen_events_df"]["lat"] == dim_location["lat"]), "inner") \
            .select("time_id", "date_id", "song_id", "userId", "location_id", "sessionId", "itemInSession", "auth", "userAgent")
    date_ids = list(incremental_fact_listen.select("date_id").distinct().toPandas()['date_id'])
    fact_listen = processor.extract_fact_table("fact_listen", date_ids).select("time_id", "date_id", "song_id", "userId", "location_id", "sessionId", "itemInSession", "auth", "userAgent")
    incremental_fact_listen = incremental_fact_listen.subtract(fact_listen)
    
    incremental_fact_auth = auth_events_dict["auth_events_df"].join(dim_time, (auth_events_dict["auth_events_df"]["hour"] == dim_time["hour"]) & (auth_events_dict["auth_events_df"]["minute"] == dim_time["minute"]) & (auth_events_dict["auth_events_df"]["second"] == dim_time["second"]), "inner") \
            .join(dim_date, (auth_events_dict["auth_events_df"]["day"] == dim_date["day"]) & (auth_events_dict["auth_events_df"]["dayOfWeek"] == dim_date["dayOfWeek"]) & (auth_events_dict["auth_events_df"]["week"] == dim_date["week"]) & (auth_events_dict["auth_events_df"]["month"] == dim_date["month"]) & (auth_events_dict["auth_events_df"]["year"] == dim_date["year"]) & (auth_events_dict["auth_events_df"]["quarter"] == dim_date["quarter"]), "inner") \
            .join(dim_user, auth_events_dict["auth_events_df"]["userId"] == dim_user["userId"], "inner").drop(auth_events_dict["auth_events_df"].userId) \
            .join(dim_location, (auth_events_dict["auth_events_df"]["city"] == dim_location["city"]) & (auth_events_dict["auth_events_df"]["zip"] == dim_location["zip"]) & (auth_events_dict["auth_events_df"]["state"] == dim_location["state"]) & (auth_events_dict["auth_events_df"]["lon"] == dim_location["lon"]) & (auth_events_dict["auth_events_df"]["lat"] == dim_location["lat"]), "inner") \
            .select("time_id", "date_id", "userId", "location_id", "sessionId", "itemInSession", "userAgent", "success")
    date_ids = list(incremental_fact_auth.select("date_id").distinct().toPandas()['date_id'])
    fact_auth = processor.extract_fact_table("fact_auth", date_ids).select("time_id", "date_id", "userId", "location_id", "sessionId", "itemInSession", "userAgent", "success")
    incremental_fact_auth = incremental_fact_auth.subtract(fact_auth)
    
    incremental_fact_page_view = page_view_events_dict["page_view_events_df"].join(dim_time, (page_view_events_dict["page_view_events_df"]["hour"] == dim_time["hour"]) & (page_view_events_dict["page_view_events_df"]["minute"] == dim_time["minute"]) & (page_view_events_dict["page_view_events_df"]["second"] == dim_time["second"]), "inner") \
            .join(dim_date, (page_view_events_dict["page_view_events_df"]["day"] == dim_date["day"]) & (page_view_events_dict["page_view_events_df"]["dayOfWeek"] == dim_date["dayOfWeek"]) & (page_view_events_dict["page_view_events_df"]["week"] == dim_date["week"]) & (page_view_events_dict["page_view_events_df"]["month"] == dim_date["month"]) & (page_view_events_dict["page_view_events_df"]["year"] == dim_date["year"]) & (page_view_events_dict["page_view_events_df"]["quarter"] == dim_date["quarter"]), "inner") \
            .join(dim_song, (page_view_events_dict["page_view_events_df"]["song"] == dim_song["title"]) & (page_view_events_dict["page_view_events_df"]["artist"] == dim_song["artist"]) & (page_view_events_dict["page_view_events_df"]["duration"] == dim_song["duration"]), "inner") \
            .join(dim_user, page_view_events_dict["page_view_events_df"]["userId"] == dim_user["userId"], "inner").drop(page_view_events_dict["page_view_events_df"].userId) \
            .join(dim_location, (page_view_events_dict["page_view_events_df"]["city"] == dim_location["city"]) & (page_view_events_dict["page_view_events_df"]["zip"] == dim_location["zip"]) & (page_view_events_dict["page_view_events_df"]["state"] == dim_location["state"]) & (page_view_events_dict["page_view_events_df"]["lon"] == dim_location["lon"]) & (page_view_events_dict["page_view_events_df"]["lat"] == dim_location["lat"]), "inner") \
            .select("time_id", "date_id", "song_id", "userId", "location_id", "sessionId", "itemInSession", "page", "auth", "method", "status", "userAgent")
    date_ids = list(incremental_fact_page_view.select("date_id").distinct().toPandas()['date_id'])
    fact_page_view = processor.extract_fact_table("fact_page_view", date_ids).select("time_id", "date_id", "song_id", "userId", "location_id", "sessionId", "itemInSession", "page", "auth", "method", "status", "userAgent")
    incremental_fact_page_view = incremental_fact_page_view.subtract(fact_page_view)
    
    # Load data to hdfs
    processor.load_to_hdfs(incremental_fact_listen, "fact_listen", "append", "date_id")
    processor.load_to_hdfs(incremental_fact_auth, "fact_auth", "append", "date_id")
    processor.load_to_hdfs(incremental_fact_page_view, "fact_page_view", "append", "date_id")
    
    processor.load_to_hdfs(incremental_dim_date, "dim_date", "append")
    processor.load_to_hdfs(incremental_dim_location, "dim_location", "append")
    processor.load_to_hdfs(incremental_dim_song, "dim_song", "append")
    processor.load_to_hdfs(incremental_dim_user, "dim_user", "append")
    

if __name__ == '__main__':
    processor = Batch_Processor()
    batch_etl(processor)