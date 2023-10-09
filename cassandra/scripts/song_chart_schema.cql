CREATE KEYSPACE IF NOT EXISTS music_streaming 
WITH REPLICATION = {
    'class': 'SimpleStrategy',
    'replication_factor': 2
};

CREATE TABLE IF NOT EXISTS music_streaming.song_chart_minute (
    song text,
    count double,
    year int,
    month int,
    week int,
    day int,
    hour int,
    minute int,
    ingest_ts timestamp,
    PRIMARY KEY ((song), ingest_ts)
)
WITH CLUSTERING ORDER BY (ingest_ts DESC);