from streaming_processor import Streaming_Processor

if __name__ == '__main__':
    spark = Streaming_Processor()
    # Process Stream Listen Events
    listen_events = spark.extract_from_kafka("listen_events")
    listen_events_hdfs = spark.transform_data(listen_events, "listen_events")
    listen_events_hdfs = spark.load_to_hdfs(listen_events_hdfs, "listen_events", "1 minute")
    
    listen_count_minute = spark.transform_listen_count(listen_events, "listen_events")
    listen_count_minute = spark.load_to_cassandra(listen_count_minute, "listen_count_minute", "1 minute")

    song_chart_minute = spark.transform_song_chart(listen_events, "listen_events")
    song_chart_minute = spark.load_to_cassandra(song_chart_minute, "song_chart_minute", "1 minute")

    artist_chart_minute = spark.transform_artist_chart(listen_events, "listen_events")
    artist_chart_minute = spark.load_to_cassandra(artist_chart_minute, "artist_chart_minute", "1 minute")
    
    
    
    # Process Stream Auth Events
    auth_events = spark.extract_from_kafka("auth_events")
    auth_events_hdfs = spark.transform_data(auth_events, "auth_events")
    auth_events_hdfs = spark.load_to_hdfs(auth_events_hdfs, "auth_events", "1 minute")

    gender_distribution = spark.transform_gender_distribution_minute(auth_events, topic="auth_events")
    gender_distribution = spark.load_to_cassandra(gender_distribution, "gender_distribution_minute", "1 minute")

    user_login = spark.transform_user_login(auth_events, topic="auth_events")
    user_login = spark.load_to_cassandra(user_login, "user_login", "1 minute")
    
    # Process Stream Page View Events
    page_view_events = spark.extract_from_kafka("page_view_events")
    page_view_events_hdfs = spark.transform_data(page_view_events, "page_view_events")
    page_view_events_hdfs = spark.load_to_hdfs(page_view_events_hdfs, "page_view_events", "1 minute")
   
    user_activity_minute = spark.transform_user_activity_count(page_view_events, "page_view_events")
    user_activity_minute = spark.load_to_cassandra(user_activity_minute, "user_activity_minute", "1 minute")
    
    
    # Start streaming to cassandra
    listen_count_minute.start()
    song_chart_minute.start()
    artist_chart_minute.start()
    
    user_activity_minute.start()
    gender_distribution.start()
    user_login.start()
    
    # Start streaming to hdfs
    listen_events_hdfs.start()
    auth_events_hdfs.start()
    page_view_events_hdfs.start()
    
    
    spark.stop()

    
