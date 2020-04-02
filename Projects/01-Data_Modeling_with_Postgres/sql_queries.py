# DROP TABLES

user_table_drop = "DROP TABLE IF EXISTS users ;"
song_table_drop = "DROP TABLE IF EXISTS songs ;"
artist_table_drop = "DROP TABLE IF EXISTS artists ;"
time_table_drop = "DROP TABLE IF EXISTS time ;"
songplay_table_drop_2 = "DROP TABLE IF EXISTS songplays ;"
songplay_table_drop = "DROP TABLE IF EXISTS song_plays ;"

# CREATE TABLES

artist_table_create = ("""
    CREATE TABLE artists (
        id TEXT PRIMARY KEY,
        name TEXT,
        location TEXT,
        latitude REAL,
        longitude REAL
    ) ;
""")

song_table_create = ("""
    CREATE TABLE songs (
        id TEXT PRIMARY KEY,
        title TEXT,
        artist_id TEXT,
        year INT,
        duration REAL
    ) ;
""")

time_table_create = ("""
    CREATE TABLE time (
        start_time TIMESTAMP(3) PRIMARY KEY,
        hour INT,
        day INT,
        week INT,
        month INT,
        year INT,
        weekday TEXT
    ) ;
""")

user_table_create = ("""
    CREATE TABLE users (
        id INT PRIMARY KEY,
        first_name TEXT,
        last_name TEXT,
        gender CHAR,
        level CHAR(4)
    ) ;
""")

songplay_table_create = ("""
    CREATE TABLE songplays (
        songplay_id SERIAL PRIMARY KEY,
        start_time VARCHAR(256),
        user_id TEXT,
        level TEXT,
        song_id TEXT,
        artist_id TEXT,
        session_id TEXT,
        location TEXT,
        user_agent TEXT
    ) ; 
""")

# INSERT RECORDS

songplay_table_insert = ("""
    INSERT INTO songplays (
        start_time, user_id, level, song_id, artist_id, session_id, location, user_agent
        )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s) ;
""")

user_table_insert = ("""
    INSERT INTO users (
        id, first_name, last_name, gender, level
        )
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT (id)
    DO UPDATE SET level=EXCLUDED.level ;
""")

song_table_insert = ("""
    INSERT INTO songs (
        id, title, artist_id, year, duration
        )
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT (id)
    DO NOTHING ;
""")

artist_table_insert = ("""
    INSERT INTO artists (
        id, name, location, latitude, longitude
        )
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT (id)
    DO NOTHING ;
""")


time_table_insert = ("""
    INSERT INTO time (
    start_time, hour, day, week, month, year, weekday
        )
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (start_time)
    DO NOTHING ;
""")

# FIND SONGS

song_select = ("""
    SELECT s.id AS song_id, a.id AS artist_id
    FROM songs AS s
    JOIN artists AS a ON s.artist_id = a.id
    WHERE s.title = %s
        AND a.name = %s
        AND s.duration = %s
""")

# QUERY LISTS

create_table_queries = [user_table_create, song_table_create, artist_table_create, time_table_create, songplay_table_create]
drop_table_queries = [user_table_drop, song_table_drop, artist_table_drop, time_table_drop, songplay_table_drop, songplay_table_drop_2]