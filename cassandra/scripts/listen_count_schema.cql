CREATE KEYSPACE IF NOT EXISTS music_streaming 
WITH REPLICATION = {
    'class': 'SimpleStrategy',
    'replication_factor': 1
};
CREATE TABLE IF NOT EXISTS music_streaming.listen_count_minute (
    count double,
    year int,
    month int,
    week int,
    day int,
    hour int,
    minute int,
    ingest_ts timestamp,
    PRIMARY KEY ((year, month, week, day, hour, minute), ingest_ts)
)
WITH CLUSTERING ORDER BY (ingest_ts DESC);