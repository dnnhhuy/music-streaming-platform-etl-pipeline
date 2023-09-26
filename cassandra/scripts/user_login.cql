CREATE KEYSPACE IF NOT EXISTS music_streaming 
WITH REPLICATION = {
    'class': 'SimpleStrategy',
    'replication_factor': 2
};

CREATE TABLE IF NOT EXISTS music_streaming.user_login (
    "userId" text,
    "lastName" text,
    "firstName" text,
    "gender" text,
    "level" text,
    "registration" timestamp,
    "ts" timestamp,
    PRIMARY KEY (("userId"), "ts")
)
WITH CLUSTERING ORDER BY ("ts" DESC);