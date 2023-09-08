CREATE TABLE IF NOT EXISTS music_streaming.song_charts (
    song text,
    count double,
    ts timestamp,
    PRIMARY KEY (song, ts)
)
WITH CLUSTERING ORDER BY (ts DESC);