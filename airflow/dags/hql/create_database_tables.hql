create database if not exists music_streaming;
use music_streaming;

create external table if not exists dim_time (
    time_id string,
    hour int,
    minute int,
    second int
)
stored as parquet location 'hdfs://namenode:9000/transformed_data/dim_time.parquet';

create external table if not exists dim_date (
    date_id string,
    day int,
    dayOfWeek int,
    week int,
    month int,
    year int,
    quarter int
)
stored as parquet location 'hdfs://namenode:9000/transformed_data/dim_date.parquet';

create external table if not exists dim_song (
    song_id string,
    title string,
    artist string,
    duration float
)
stored as parquet location 'hdfs://namenode:9000/transformed_data/dim_song.parquet';

create external table if not exists dim_location (
    location_id string,
    city string,
    zip string,
    state string,
    lon double,
    lat double
)
stored as parquet location 'hdfs://namenode:9000/transformed_data/dim_location.parquet';

create external table if not exists dim_user (
    userId string,
    lastName string,
    firstName string,
    gender string,
    level string,
    registration timestamp
)
stored as parquet location 'hdfs://namenode:9000/transformed_data/dim_user.parquet';


create external table if not exists fact_listen(
    time_id string,
    date_id string,
    song_id string,
    userId string,
    location_id string,
    sessionId bigint,
    itemInSession int,
    auth string,
    userAgent string
)
stored as parquet location 'hdfs://namenode:9000/transformed_data/fact_listen.parquet';

create external table if not exists fact_auth(
    time_id string,
    date_id string,
    userId string,
    location_id string,
    sessionId bigint,
    itemInSession int,
    userAgent string,
    success boolean
)
stored as parquet location 'hdfs://namenode:9000/transformed_data/fact_auth.parquet';

create external table if not exists fact_page_view(
    time_id string,
    date_id string,
    song_id string,
    userId string,
    location_id string,
    sessionId bigint,
    itemInSession int,
    page string,
    auth string,
    method string,
    status string,
    userAgent string
)
stored as parquet location 'hdfs://namenode:9000/transformed_data/fact_page_view.parquet';