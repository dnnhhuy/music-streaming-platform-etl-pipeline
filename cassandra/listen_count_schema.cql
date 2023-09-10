CREATE KEYSPACE IF NOT EXISTS music_streaming 
WITH REPLICATION = {
    'class': 'SimpleStrategy',
    'replication_factor': 1
};
CREATE TABLE IF NOT EXISTS music_streaming.listen_count (
    listen_count text,
    count double,
    ts timestamp,
    PRIMARY KEY ((listen_count), ts)
)
WITH CLUSTERING ORDER BY (ts DESC);

