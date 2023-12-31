from batch_processor import Batch_Processor
from pyspark.sql import functions as func
from utils import *

def batch_etl(processor):
    # Extract data
    # Only extract data generated before current day
    listen_events_df = processor.extract_data("listen_events", "full").filter(func.col("date") < func.to_date(func.current_date()))
    auth_events_df = processor.extract_data("auth_events", "full").filter(func.col("date") < func.to_date(func.current_date()))
    page_view_events_df = processor.extract_data("page_view_events", "full").filter(func.col("date") < func.to_date(func.current_date())) \
            .withColumn("registration", func.when(func.col("registration").isNotNull(), func.col("registration")).otherwise(func.lit("empty"))) \
            .na.fill(0.0, subset=["duration"]) \
            .withColumn("userId", func.coalesce(func.col("userId"), func.when(func.col("level")=="free", "0").otherwise("1"))) \
            .na.fill("empty").persist()
            
    # Transform data
    listen_events_dict = processor.transform_listen_events(listen_events_df)
    auth_events_dict = processor.transform_auth_events(auth_events_df)
    page_view_events_dict = processor.transform_page_view_events(page_view_events_df)
    
    dim_time = processor.create_dim_time().persist()
    
    dim_date = unionAll([listen_events_dict["dim_date"], auth_events_dict["dim_date"], page_view_events_dict["dim_date"]]) \
                .distinct() \
                .withColumn("date_id", func.expr("uuid()")) \
                .persist()
                
    dim_user = unionAll([listen_events_dict["dim_user"], auth_events_dict["dim_user"], page_view_events_dict["dim_user"]]) \
                .distinct() \
                .persist()
                
    dim_location = unionAll([listen_events_dict["dim_location"], auth_events_dict["dim_location"], page_view_events_dict["dim_location"]]) \
                .distinct() \
                .withColumn("location_id", func.expr("uuid()")) \
                .persist()
                
    dim_song = unionAll([listen_events_dict["dim_song"], page_view_events_dict["dim_song"]]) \
                .distinct() \
                .withColumn("song_id", func.expr("uuid()")) \
                .persist()
    
    fact_listen = listen_events_dict["listen_events_df"].join(dim_time, (listen_events_dict["listen_events_df"]["hour"] == dim_time["hour"]) & (listen_events_dict["listen_events_df"]["minute"] == dim_time["minute"]) & (listen_events_dict["listen_events_df"]["second"] == dim_time["second"]), "inner") \
            .join(dim_date, (listen_events_dict["listen_events_df"]["day"] == dim_date["day"]) & (listen_events_dict["listen_events_df"]["dayOfWeek"] == dim_date["dayOfWeek"]) & (listen_events_dict["listen_events_df"]["week"] == dim_date["week"]) & (listen_events_dict["listen_events_df"]["month"] == dim_date["month"]) & (listen_events_dict["listen_events_df"]["year"] == dim_date["year"]) & (listen_events_dict["listen_events_df"]["quarter"] == dim_date["quarter"]), "inner") \
            .join(dim_song, (listen_events_dict["listen_events_df"]["song"] == dim_song["title"]) & (listen_events_dict["listen_events_df"]["artist"] == dim_song["artist"]) & (listen_events_dict["listen_events_df"]["duration"] == dim_song["duration"]), "inner") \
            .join(dim_user, listen_events_dict["listen_events_df"]["userId"] == dim_user["userId"], "inner").drop(listen_events_dict["listen_events_df"].userId) \
            .join(dim_location, (listen_events_dict["listen_events_df"]["city"] == dim_location["city"]) & (listen_events_dict["listen_events_df"]["zip"] == dim_location["zip"]) & (listen_events_dict["listen_events_df"]["state"] == dim_location["state"]) & (listen_events_dict["listen_events_df"]["lon"] == dim_location["lon"]) & (listen_events_dict["listen_events_df"]["lat"] == dim_location["lat"]), "inner") \
            .select("time_id", "date_id", "song_id", func.col("userId"), "location_id", "sessionId", "itemInSession", "auth", "userAgent")
    
    fact_auth = auth_events_dict["auth_events_df"].join(dim_time, (auth_events_dict["auth_events_df"]["hour"] == dim_time["hour"]) & (auth_events_dict["auth_events_df"]["minute"] == dim_time["minute"]) & (auth_events_dict["auth_events_df"]["second"] == dim_time["second"]), "inner") \
            .join(dim_date, (auth_events_dict["auth_events_df"]["day"] == dim_date["day"]) & (auth_events_dict["auth_events_df"]["dayOfWeek"] == dim_date["dayOfWeek"]) & (auth_events_dict["auth_events_df"]["week"] == dim_date["week"]) & (auth_events_dict["auth_events_df"]["month"] == dim_date["month"]) & (auth_events_dict["auth_events_df"]["year"] == dim_date["year"]) & (auth_events_dict["auth_events_df"]["quarter"] == dim_date["quarter"]), "inner") \
            .join(dim_user, auth_events_dict["auth_events_df"]["userId"] == dim_user["userId"], "inner").drop(auth_events_dict["auth_events_df"].userId) \
            .join(dim_location, (auth_events_dict["auth_events_df"]["city"] == dim_location["city"]) & (auth_events_dict["auth_events_df"]["zip"] == dim_location["zip"]) & (auth_events_dict["auth_events_df"]["state"] == dim_location["state"]) & (auth_events_dict["auth_events_df"]["lon"] == dim_location["lon"]) & (auth_events_dict["auth_events_df"]["lat"] == dim_location["lat"]), "inner") \
            .select("time_id", "date_id", 'userId', "location_id", "sessionId", "itemInSession", "userAgent", "success")
    
    fact_page_view = page_view_events_dict["page_view_events_df"].join(dim_time, (page_view_events_dict["page_view_events_df"]["hour"] == dim_time["hour"]) & (page_view_events_dict["page_view_events_df"]["minute"] == dim_time["minute"]) & (page_view_events_dict["page_view_events_df"]["second"] == dim_time["second"]), "inner") \
            .join(dim_date, (page_view_events_dict["page_view_events_df"]["day"] == dim_date["day"]) & (page_view_events_dict["page_view_events_df"]["dayOfWeek"] == dim_date["dayOfWeek"]) & (page_view_events_dict["page_view_events_df"]["week"] == dim_date["week"]) & (page_view_events_dict["page_view_events_df"]["month"] == dim_date["month"]) & (page_view_events_dict["page_view_events_df"]["year"] == dim_date["year"]) & (page_view_events_dict["page_view_events_df"]["quarter"] == dim_date["quarter"]), "inner") \
            .join(dim_song, (page_view_events_dict["page_view_events_df"]["song"] == dim_song["title"]) & (page_view_events_dict["page_view_events_df"]["artist"] == dim_song["artist"]) & (page_view_events_dict["page_view_events_df"]["duration"] == dim_song["duration"]), "inner") \
            .join(dim_user, page_view_events_dict["page_view_events_df"]["userId"] == dim_user["userId"], "inner").drop(page_view_events_dict["page_view_events_df"].userId) \
            .join(dim_location, (page_view_events_dict["page_view_events_df"]["city"] == dim_location["city"]) & (page_view_events_dict["page_view_events_df"]["zip"] == dim_location["zip"]) & (page_view_events_dict["page_view_events_df"]["state"] == dim_location["state"]) & (page_view_events_dict["page_view_events_df"]["lon"] == dim_location["lon"]) & (page_view_events_dict["page_view_events_df"]["lat"] == dim_location["lat"]), "inner") \
            .select("time_id", "date_id", "song_id", 'userId', "location_id", "sessionId", "itemInSession", "page", "auth", "method", "status", "userAgent")
    
    # Load data to hdfs
    processor.load_to_hdfs(fact_listen, "fact_listen", "overwrite", "date_id")
    processor.load_to_hdfs(fact_auth, "fact_auth", "overwrite", "date_id")
    processor.load_to_hdfs(fact_page_view, "fact_page_view", "overwrite", "date_id")
    
    processor.load_to_hdfs(dim_date, "dim_date", "overwrite")
    processor.load_to_hdfs(dim_location, "dim_location", "overwrite")
    processor.load_to_hdfs(dim_song, "dim_song", "overwrite")
    processor.load_to_hdfs(dim_time, "dim_time", "overwrite")
    processor.load_to_hdfs(dim_user, "dim_user", "overwrite")
    

if __name__ == '__main__':
    processor = Batch_Processor()
    batch_etl(processor)