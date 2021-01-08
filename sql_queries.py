import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplay"
user_table_drop = "DROP TABLE IF EXISTS sparkify_user"
song_table_drop = "DROP TABLE IF EXISTS song"
artist_table_drop = "DROP TABLE IF EXISTS artist"
time_table_drop = "DROP TABLE IF EXISTS start_time"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE staging_events 
(
    artist          VARCHAR(300),
    auth            VARCHAR(25),
    first_name      VARCHAR(25),
    gender          VARCHAR(1),
    item_in_session INTEGER, 
    last_name       VARCHAR(25),
    legnth          DECIMAL(9, 5),
    level           VARCHAR(10),
    location        VARCHAR(300),
    method          VARCHAR(6),
    page            VARCHAR(50),
    registration    DECIMAL(14, 1),
    session_id      INTEGER,
    song            VARCHAR(300),
    status          INTEGER,
    ts              BIGINT,
    user_agent      VARCHAR(150),
    user_id         VARCHAR(10)
)
""")

staging_songs_table_create = ("""
CREATE TABLE staging_songs 
(
    num_songs        INTEGER,
    artist_id        VARCHAR(25), 
    artist_latitude  DECIMAL(10, 5),
    artist_longitude DECIMAL(10, 5),
    artist_location  VARCHAR(300),
    artist_name      VARCHAR(300),
    song_id          VARCHAR(25),
    title            VARCHAR(300),
    duration         DECIMAL(9, 5),
    year             INTEGER
)
""")

songplay_table_create = ("""
CREATE TABLE songplay 
(
    songplay_id INTEGER IDENTITY(0,1) PRIMARY KEY,
    start_time  TIMESTAMP NOT NULL, 
    user_id     VARCHAR(10),
    level       VARCHAR(10),
    song_id     VARCHAR(300) NOT NULL,
    artist_id   VARCHAR(25) NOT NULL,
    session_id  INTEGER,
    location    VARCHAR(300),
    user_agent  VARCHAR(150)
)
""")

user_table_create = ("""
CREATE TABLE sparkify_user 
(
    user_id    VARCHAR(10) PRIMARY KEY,
    first_name VARCHAR(25),
    last_name  VARCHAR(25),
    gender     VARCHAR(1),
    level      VARCHAR(10)
)
""")

song_table_create = ("""
CREATE TABLE song 
(
    song_id   VARCHAR(25) PRIMARY KEY,
    title     VARCHAR(300) NOT NULL,
    artist_id VARCHAR(25),
    year      INTEGER,
    duration  DECIMAL(9, 5) NOT NULL
)
""")

artist_table_create = ("""
CREATE TABLE artist 
(
    artist_id VARCHAR(25) PRIMARY KEY,
    name      VARCHAR(300) NOT NULL,
    location  VARCHAR(300),
    lattitude DECIMAL(10, 5),
    longitude DECIMAL(10, 5)
)
""")

time_table_create = ("""
CREATE TABLE start_time 
(
    start_time TIMESTAMP PRIMARY KEY,
    hour       INTEGER,
    day        INTEGER,
    week       INTEGER,
    month      INTEGER,
    year       INTEGER,
    weekday    INTEGER
)
""")

# STAGING TABLES

staging_events_copy = ("""
COPY staging_events FROM {} 
IAM_ROLE {}
REGION 'us-west-2'
COMPUPDATE OFF
FORMAT AS JSON {};
""").format(
    config.get('S3', 'LOG_DATA'), 
    config.get('IAM_ROLE', 'ARN'), 
    config.get('S3', 'LOG_JSONPATH'))

staging_songs_copy = ("""
COPY staging_songs FROM {} 
IAM_ROLE {}
FORMAT AS JSON 'auto';
""").format(
    config.get('S3', 'SONG_DATA'), 
    config.get('IAM_ROLE', 'ARN'))

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays ( start_time,
                        user_id,
                        level,
                        song_id,
                        artist_id,
                        session_id,
                        location,
                        user_agent)
SELECT  DISTINCT TIMESTAMP 'epoch' + se.ts/1000 \
            * INTERVAL '1 second'   AS start_time,
        se.userId                   AS user_id,
        se.level                    AS level,
        ss.song_id                  AS song_id,
        ss.artist_id                AS artist_id,
        se.sessionId                AS session_id,
        se.location                 AS location,
        se.userAgent                AS user_agent
FROM staging_events AS se
JOIN staging_songs AS ss
    ON (se.artist = ss.artist_name)
WHERE se.page = 'NextSong';
""")

user_table_insert = ("""
INSERT INTO users ( user_id,
                    first_name,
                    last_name,
                    gender,
                    level)
SELECT  DISTINCT se.userId          AS user_id,
        se.firstName                AS first_name,
        se.lastName                 AS last_name,
        se.gender                   AS gender,
        se.level                    AS level
FROM staging_events AS se
WHERE se.page = 'NextSong';
""")

song_table_insert = ("""
INSERT INTO songs ( song_id,
                    title,
                    artist_id,
                    year,
                    duration)
SELECT  DISTINCT ss.song_id         AS song_id,
        ss.title                    AS title,
        ss.artist_id                AS artist_id,
        ss.year                     AS year,
        ss.duration                 AS duration
FROM staging_songs AS ss;
""")

artist_table_insert = ("""
INSERT INTO artists ( artist_id,
                      name,
                      location,
                      latitude,
                      longitude)
SELECT  DISTINCT ss.artist_id   AS artist_id,
        ss.artist_name          AS name,
        ss.artist_location      AS location,
        ss.artist_latitude      AS latitude,
        ss.artist_longitude     AS longitude
FROM staging_songs AS ss;
""")

time_table_insert = ("""
INSERT INTO start_time(
    start_time,
    hour,
    day,
    week,
    month,
    year,
    weekday
)
SELECT  DISTINCT TIMESTAMP 'epoch' + se.ts/1000 \
            * INTERVAL '1 second'        AS start_time,
        EXTRACT(hour FROM start_time)    AS hour,
        EXTRACT(day FROM start_time)     AS day,
        EXTRACT(week FROM start_time)    AS week,
        EXTRACT(month FROM start_time)   AS month,
        EXTRACT(year FROM start_time)    AS year,
        EXTRACT(week FROM start_time)    AS weekday
    FROM    staging_events                   AS se
    WHERE se.page = 'NextSong';
""")
# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]

