import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

IAM_ROLE = config['IAM_ROLE']['ARN']
LOG_DATA = config['S3']['LOG_DATA']
LOG_JSONPATH = config['S3']['LOG_JSONPATH']
SONG_DATA = config['S3']['SONG_DATA']

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS STG_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS STG_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""
                        CREATE TABLE IF NOT EXISTS STG_events
                        (
                                artist varchar,
                                auth varchar,
                                firstName varchar,
                                gender varchar,
                                itemInSession varchar,
                                lastName varchar,
                                length varchar,
                                level varchar,
                                location varchar,
                                method varchar,
                                page varchar,
                                registration varchar,
                                sessionId varchar,
                                song varchar,
                                status varchar,
                                ts varchar,
                                userAgent varchar,
                                userId varchar
                        )


""")

staging_songs_table_create = ("""
                        CREATE TABLE IF NOT EXISTS STG_songs
                        (
                                num_songs int,
                                artist_id text,
                                artist_latitude text,
                                artist_longitude text,
                                artist_location text,
                                artist_name text,
                                song_id text,
                                title text,
                                duration float,
                                year int
                        )
""")

songplay_table_create = ("""
                        Create table if not exists songplays
                        (
                                songplay_id int IDENTITY(0,1), 
                                start_time timestamp NOT NULL, 
                                user_id text NOT NULL, 
                                level text, 
                                song_id text, 
                                artist_id text, 
                                session_id text, 
                                location text, 
                                user_agent text
)
""")

user_table_create = ("""
Create table if not exists users

(user_id text,
first_name text,
last_name text,
gender text,
level text)
""")

song_table_create = ("""
Create table if not exists songs

(song_id text NOT NULL,
title text,
artist_id text,
year int,
duration float)

""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists 
                            (
                                artist_id text NOT NULL, 
                                name text, 
                                location text, 
                                latitude numeric, 
                                longitude numeric
                                )
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time 
                            (
                                start_time timestamp NOT NULL, 
                                hour int, 
                                day int, 
                                week int, 
                                month int, 
                                year int, 
                                weekday varchar
                                )
""")

# STAGING TABLES

staging_events_copy = ("""copy STG_events from {} 
credentials 'aws_iam_role={}'
format as json {}
region 'us-west-2';
""").format(LOG_DATA,IAM_ROLE,LOG_JSONPATH)

staging_songs_copy = ("""copy STG_songs from {} 
credentials 'aws_iam_role={}'
format as json 'auto'
region 'us-west-2';
""").format(SONG_DATA, IAM_ROLE)

# FINAL TABLES

songplay_table_insert = ("""INSERT INTO songplays 
                            (
                                start_time, 
                                user_id, 
                                level, 
                                song_id, 
                                artist_id, 
                                session_id, 
                                location, 
                                user_agent
                            )
                            SELECT DISTINCT
                            TIMESTAMP 'epoch' + (e.ts / 1000) * INTERVAL '1 second' AS start_time,
                            e.userId,
                            e.level,
                            s.song_id,
                            s.artist_id,
                            e.sessionId,
                            s.artist_location,
                            e.userAgent
                            FROM stg_events e
                            JOIN stg_songs s
                            ON (e.artist = s.artist_name) AND (e.song = s.title)
""")

user_table_insert = ("""INSERT INTO users 
                            (
                                user_id, 
                                first_name, 
                                last_name, 
                                gender, 
                                level
                            )
                            SELECT DISTINCT
                            e.userId,
                            e.firstName,
                            e.lastName,
                            e.gender,
                            e.level
                            from STG_events e
                            
""")

song_table_insert = ("""INSERT INTO songs 
                            (
                                song_id, 
                                title, 
                                artist_id, 
                                year, 
                                duration
                            )
                            SELECT DISTINCT
                            s.song_id,
                            s.title,
                            s.artist_id,
                            s.year,
                            s.duration
                            from STG_songs s
""")

artist_table_insert = ("""INSERT INTO artists 
                            (
                                artist_id, 
                                name, 
                                location, 
                                latitude, 
                                longitude
                            )
                            SELECT DISTINCT
                            s.artist_id,
                            s.artist_name,
                            s.artist_location,
                            s.artist_latitude,
                            s.artist_longitude
                            from STG_songs s
""")

time_table_insert = ("""INSERT INTO time 
                            (
                                start_time, 
                                hour, 
                                day, 
                                week, 
                                month, 
                                year,
                                weekday
                            )
                            SELECT DISTINCT 
                            TIMESTAMP 'epoch' + (e.ts / 1000) * INTERVAL '1 second' AS start_time,
                            EXTRACT(HOUR FROM start_time) AS hour,
                            EXTRACT(DAY FROM start_time) AS day,
                            EXTRACT(WEEK FROM start_time) AS week,
                            EXTRACT(MONTH FROM start_time) AS month,
                            EXTRACT(YEAR FROM start_time) AS year,
                            EXTRACT(DOW FROM start_time) AS weekday
                            FROM stg_events e
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
