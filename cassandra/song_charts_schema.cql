CREATE KEYSPACE IF NOT EXISTS music_streaming 
WITH REPLICATION = {
    'class': 'SimpleStrategy',
    'replication_factor': 1
};
CREATE TABLE IF NOT EXISTS music_streaming.song_charts_minute (
    song text,
    count double,
    year int,
    month int,
    day int,
    hour int,
    minute int,
    PRIMARY KEY ((song), year, month, day, hour, minute)
)
WITH CLUSTERING ORDER BY (year DESC, month DESC, day DESC, hour DESC, minute DESC);

CREATE TABLE IF NOT EXISTS music_streaming.song_charts_hour (
    song text,
    count double,
    ts timestamp,
    year int,
    month int,
    day int,
    hour int,
    PRIMARY KEY ((song), year, month, day, hour)
)
WITH CLUSTERING ORDER BY (year DESC, month DESC, day DESC, hour DESC);

