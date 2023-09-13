CREATE KEYSPACE IF NOT EXISTS music_streaming 
WITH REPLICATION = {
    'class': 'SimpleStrategy',
    'replication_factor': 1
};

CREATE TABLE IF NOT EXISTS music_streaming.artist_chart_minute (
    artist text,
    count double,
    year int,
    month int,
    day int,
    hour int,
    minute int,
    ingest_ts timestamp,
    PRIMARY KEY ((artist), ingest_ts)
)
WITH CLUSTERING ORDER BY (ingest_ts DESC);