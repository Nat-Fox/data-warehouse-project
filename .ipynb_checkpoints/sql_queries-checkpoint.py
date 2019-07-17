import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP table IF EXISTS staging_events;"
staging_songs_table_drop = "DROP table IF EXISTS staging_songs;"
songplay_table_drop = "DROP table IF EXISTS songplays;"
user_table_drop = "DROP table IF EXISTS users;"
song_table_drop = "DROP table IF EXISTS songs;"
artist_table_drop = "DROP table IF EXISTS artists;"
time_table_drop = "DROP table IF EXISTS time;"


# CREATE TABLES
staging_events_table_create= ("""CREATE TABLE IF NOT EXISTS staging_events (artist VARCHAR, auth VARCHAR, first_name VARCHAR, gender VARCHAR, item_in_session INTEGER, last_name VARCHAR, length NUMERIC, level VARCHAR, location VARCHAR, method VARCHAR, page VARCHAR, registration BIGINT, session_id INTEGER, song VARCHAR, status INTEGER, ts TIMESTAMP, user_agent VARCHAR, user_id INTEGER)
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs (num_songs INTEGER, artist_id VARCHAR, artist_latitude NUMERIC, artist_longitude NUMERIC, artist_location VARCHAR, artist_name VARCHAR, song_id VARCHAR, title VARCHAR, duration NUMERIC, year INTEGER)
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays (songplay_id int IDENTITY(0,1) PRIMARY KEY, start_time TIMESTAMP NOT NULL, user_id INTEGER NOT NULL, level VARCHAR NOT NULL, song_id VARCHAR, artist_id VARCHAR, session_id INTEGER NOT NULL, location VARCHAR NOT NULL, user_agent VARCHAR NOT NULL);
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (user_id INTEGER PRIMARY KEY, first_name VARCHAR(25), last_name VARCHAR(25), gender VARCHAR(10), level VARCHAR(20));
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs (song_id VARCHAR PRIMARY KEY, title VARCHAR, artist_id VARCHAR NOT NULL, year INTEGER, duration NUMERIC);
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists (artist_id VARCHAR PRIMARY KEY, name VARCHAR, location VARCHAR, lattitude NUMERIC, longitude NUMERIC);
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (start_time TIMESTAMP, hour INTEGER, day VARCHAR, week VARCHAR, month VARCHAR, year VARCHAR, weekday VARCHAR)
""")

# STAGING TABLES

staging_events_copy = ("""
copy staging_events from {} iam_role {} json {} region 'us-west-2' TIMEFORMAT as 'epochmillisecs';
""").format(config['S3']['LOG_DATA'], config['IAM_ROLE']['ARN'], config['S3']['LOG_JSONPATH'])

staging_songs_copy = ("""
copy staging_songs from {} iam_role {} json 'auto' region 'us-west-2';
""").format(config['S3']['SONG_DATA'], config['IAM_ROLE']['ARN'])

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent) SELECT DISTINCT ts as start_time, staging_events.user_id, staging_events.level, staging_songs.song_id, staging_songs.artist_id, staging_events.session_id, staging_events.location, staging_events.user_agent FROM staging_events, staging_songs WHERE staging_events.song = staging_songs.title
""")

user_table_insert = ("""
INSERT INTO users (user_id, first_name, last_name, gender, level) SELECT DISTINCT user_id, first_name, last_name, gender, gender FROM staging_events WHERE page = 'NextSong';
""")

song_table_insert = ("""
INSERT INTO songs (song_id, title, artist_id, year, duration) SELECT DISTINCT song_id, title, artist_id, year, duration
FROM staging_songs WHERE song_id NOT IN (SELECT DISTINCT song_id FROM songs)
""")

artist_table_insert = ("""
INSERT INTO artists (artist_id, name, location, lattitude, longitude) SELECT DISTINCT artist_id, staging_songs.artist_name as name, 
staging_songs.artist_location as location, staging_songs.artist_latitude as lattitude, staging_songs.artist_longitude as longitude
FROM staging_songs WHERE artist_id NOT IN (SELECT DISTINCT artist_id FROM artists);
""")

time_table_insert = ("""
INSERT INTO time (start_time, hour, day, week, month, year, weekday) SELECT DISTINCT ts, EXTRACT(hour from ts) as hour, EXTRACT(d from ts) as day, EXTRACT(w from ts) as week, EXTRACT(mon from ts) as month, EXTRACT(yr from ts) as year, EXTRACT(weekday from ts) AS weekday FROM staging_events
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
#drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
#copy_table_queries = []
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
