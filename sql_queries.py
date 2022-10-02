import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE the 2 staging tables TABLES to copy or data into

#we first create the staging table fro the event loggingin the app.
staging_events_table_create= ("""CREATE TABLE IF NOT EXISTS staging_events
 (
   artist   VARCHAR,
   auth   VARCHAR,
   firstName  VARCHAR,
   gender   VARCHAR,
   itemInSession   INT,
   lastName   VARCHAR,
   length   FLOAT,
   level   VARCHAR,
   location  VARCHAR,
   method   VARCHAR,
   page   VARCHAR,
   registration  VARCHAR,
   sessionId   INT,
   song   VARCHAR,
   status   INT,
   ts  TIMESTAMP,
   userAgent   VARCHAR,
   userId   INT
 );
""")

# then create the staging table for the songs metadata.
staging_songs_table_create = ("""CREATE TABLE IF NOT EXISTS staging_songs
 ( 
   num_songs  INT ,
   artist_id  VARCHAR,  
   artist_latitude  NUMERIC,
   artist_longitude NUMERIC,
   artist_location  VARCHAR,
   artist_name  VARCHAR,
   song_id  VARCHAR,
   title   VARCHAR,
   duration   FLOAT,
   year   INT
 );
""")

# Here we create the and design our database starting with the fact table
# folowed by the dimension tables

# the fact table of the songplays data
songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays
 (
    songplay_id INT IDENTITY (0,1) PRIMARY KEY,
    start_time INT NOT NULL,
    user_id   INT NOT NULL,
    level   VARCHAR,
    song_id   VARCHAR NOT NULL,
    artist_id   VARCHAR NOT NULL,
    sessionId  INT,
    location   VARCHAR,
    userAgent   VARCHAR
 )
    DISTSTYLE KEY
    DISTKEY (sessionId);
""")

#create the users data table
user_table_create = ("""CREATE TABLE IF NOT EXISTS users
 (
    user_id VARCHAR NOT NULL PRIMARY KEY,
    first_name   VARCHAR,
    last_name   VARCHAR,
    gender   VARCHAR,
    level VARCHAR
 )
    DISTSTYLE ALL;
""")

#create the songs metadata table
song_table_create = ("""CREATE TABLE IF NOT EXISTS songs
 (
  song_id VARCHAR NOT NULL PRIMARY KEY,
  title   VARCHAR,
  artist_id  VARCHAR NOT NULL,
  year  INT,
  duration  FLOAT
 )
  DISTSTYLE AUTO;
""")

#create the artists table from the songs data
artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists
 (
   artist_id VARCHAR NOT NULL PRIMARY KEY,
   name   VARCHAR,
   location   VARCHAR,
   longtitude   NUMERIC,
   longitude   NUMERIC
 )
   DISTSTYLE ALL;
""")

#finally we create the time stamp table when the logging occured
time_table_create = ("""CREATE TABLE IF NOT EXISTS time
 ( 
   start_time  TIMESTAMP NOT NULL PRIMARY KEY,
   hour   INT,
   day   INT,
   week   INT,
   month   INT,
   year   INT,
   dayOfWeek   VARCHAR
 )
   DISTSTYLE AUTO;
""")

# STAGING or copying sparify's data into our tables above

#firt we loadour event logging data
staging_events_copy = (""" COPY staging_events
                           from {}
                           iam_role {}
                           format as json {}
                           region 'us-west-2';
""").format(config['S3']['LOG_DaTA'], config['IAM_ROLE']['ARN'], config['S3']['LOG_JSONPATH'])

#secong we copy and load the song data
staging_songs_copy = (""" COPY staging_songs
                          from {}
                          iam_role {}
                          format as json 'auto'
                          region 'us-west-2';
""").format(config['S3']['SONG_DATA'], config['IAM_ROLE']['ARN'])

# Here we define the how and where we will be loading our data from the staging tables to our final tables.

#inserting the data frome event staging table and the song data table into the songplay table
songplay_table_insert = ("""INSERT INTO songplays (songplay_id, start_time, user_id, level, song_id,artist_id, session_id, location, user_agent)
                             SELECT DISTINCT
                                 se.songplay_id,
                                 se.ts,
                                 se.userId,
                                 se.level,
                                 se.song_id,
                                 ss.artist_id,
                                 se.sessionId,
                                 se.location,
                                 se.userAgent
                             FROM staging_events se
                             JOIN staging_songs ss
                             WHERE se.page = 'NextSong'
                             AND se.artist = ss.artist_name
                             AND se.song = ss.title;
""")

#inserting the data frome event staging table filtered by the timestamp where the most recent user_id will the one added.
user_table_insert = ("""INSERT INTO users(user_id, first_name, lastName, gender,level)
                        WITH uniq_staging_events AS (
                        SELECT userId, firstName, lastName, gender, level,
                           ROW_NUMBER() OVER(PARTITION BY userId ORDER BY ts DESC) AS rank
                        FROM staging_events
                                WHERE userid IS NOT NULL
                        )
                        SELECT userid, first_name, last_name, gender, level
                            FROM uniq_staging_events
                        WHERE rank = 1;
""")

#inserting the data from the staging song data table into the song table
song_table_insert = ("""INSERT INTO songs (song_id, title, artist_id, year, duration)
                         SELECT DISTINCT
                            song_id,
                            title,
                            artist_id,
                            year,
                            duration
                         FROM staging_songs
                         WHERE song_id IS NOT NULL;
""")

#inserting the data from the staging song data table into the artist table
artist_table_insert = ("""INSERT INTO artists (artist_id, name, location, lattitude, longitude)
                           SELECT DISTINCT
                              artist_id,
                              artist_name,
                              artist_location,
                              artist_latitude,
                              artist_longtitude
                           FROM staging_songs
                           WHERE artist_id IS NOT NULL;
""")                        

# extracting the time timestamp coloumn from our songplay table into a time table broked down to understandable and sortable time units.
time_table_insert = ("""INSERT INTO time (start_time, hour, day, week, month, year, weekday)

                         SELECT  start_time,
                            EXTRACT(hour from start_time) AS hour,
                            EXTRACT(day from start_time) AS day,
                            EXTRACT(week from start_time) AS week,
                            EXTRACT(month from start_time) AS month,
                            EXTRACT(year from start_time) AS year,
                            EXTRACT(dayofweek from start_time) AS weekDay,
                         FROM songplays;
""")

# QUERY LISTS - called upon and used in both the create_tables and etl python files.

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
