CREATE KEYSPACE IF NOT EXISTS music_streaming 
WITH REPLICATION = {
    'class': 'SimpleStrategy',
    'replication_factor': 2
};

CREATE TABLE IF NOT EXISTS music_streaming.user_auth (
    "userId" text,
    "lastName" text,
    "firstName" text,
    "gender" text,
    "level" text,
    "registration" timestamp,
    PRIMARY KEY ("userId")
);