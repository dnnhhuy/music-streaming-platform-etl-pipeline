CREATE KEYSPACE IF NOT EXISTS music_streaming 
WITH REPLICATION = {
    'class': 'SimpleStrategy',
    'replication_factor': 2
};

CREATE TABLE IF NOT EXISTS music_streaming.gender_distribution_minute (
    "gender" text,
    "level" text,
    "count" bigint,
    "ingest_ts" timestamp,
    PRIMARY KEY (("gender", "level"), "ingest_ts")
)
WITH CLUSTERING ORDER BY ("ingest_ts" DESC);