class SqlQueries:
    # DELETE-LOAD OPTIONS
    songplay_table_insert = """
        SELECT
            md5(events.sessionid || events.start_time) songplay_id,
            events.start_time, 
            events.userid, 
            events.level, 
            songs.song_id, 
            songs.artist_id, 
            events.sessionid, 
            events.location, 
            events.useragent
        FROM (SELECT TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time, *
            FROM staging_events
            WHERE page='NextSong') events
        LEFT JOIN staging_songs songs
        ON events.song = songs.title
            AND events.artist = songs.artist_name
            AND events.length = songs.duration
    """

    user_table_insert = """
        SELECT distinct userid, firstname, lastname, gender, level
        FROM staging_events
        WHERE page='NextSong'
    """

    song_table_insert = """
        SELECT distinct song_id, title, artist_id, year, duration
        FROM staging_songs
    """

    artist_table_insert = """
        SELECT distinct artist_id, artist_name, artist_location, artist_latitude, artist_longitude
        FROM staging_songs
    """

    time_table_insert = """
        SELECT start_time, extract(hour from start_time), extract(day from start_time), extract(week from start_time), 
               extract(month from start_time), extract(year from start_time), extract(dayofweek from start_time)
        FROM songplays
    """

    # APPEND-LOAD OPTIONS
    songplay_table_append = """
        SELECT
            md5(events.sessionid || events.start_time) songplay_id,
            events.start_time, 
            events.userid, 
            events.level, 
            songs.song_id, 
            songs.artist_id, 
            events.sessionid, 
            events.location, 
            events.useragent
        FROM (SELECT 
                TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time, 
                *
              FROM staging_events) events
        LEFT JOIN staging_songs songs
        ON events.song = songs.title
            AND events.artist = songs.artist_name
            AND events.length = songs.duration
        WHERE page='NextSong'
            AND NOT EXISTS( 
                        SELECT songplay_id
                        FROM {}
                        WHERE   songplay_id = {}.songplay_id
                        )    
    """

    user_table_append = """
        SELECT distinct userid, firstname, lastname, gender, level
        FROM staging_events
        WHERE page='NextSong'
            AND NOT EXISTS(
                        SELECT userid
                        FROM {}
                        WHERE userid = {}.userid
            )
    """

    song_table_append = """
        SELECT distinct song_id, title, artist_id, year, duration
        FROM staging_songs ss
        WHERE NOT EXISTS(
                        SELECT song_id
                        FROM {} 
                        WHERE ss.song_id = {}.song_id
        )
    """

    artist_table_append = """
        SELECT distinct artist_id, artist_name, artist_location, artist_latitude, artist_longitude
        FROM staging_songs
        WHERE NOT EXISTS(
                        SELECT artist_id
                        FROM {}
                        WHERE artist_id = {}.artist_id
        )
    """

    time_table_append = """
        SELECT start_time, extract(hour from start_time), extract(day from start_time), extract(week from start_time), 
               extract(month from start_time), extract(year from start_time), extract(dayofweek from start_time)
        FROM songplays
        WHERE NOT EXISTS(
                        SELECT start_time
                        FROM {}
                        WHERE start_time = {}.start_time
        )
    """
    
    # DATA QUALITY - CHECK NULLS
    songplays_table_nulls = """
        SELECT COUNT(*)
        FROM songplays
        WHERE   songplay_id IS NULL OR
                start_time IS NULL OR
                userid IS NULL
    """
    
    user_table_nulls = """
        SELECT COUNT(*)
        FROM users
        WHERE userid IS NULL
    """

    song_table_nulls = """
        SELECT COUNT(*)
        FROM songs
        WHERE song_id IS NULL
    """

    artist_table_nulls = """
        SELECT COUNT(*)
        FROM artists
        WHERE artist_id IS NULL
    """

    time_table_nulls = """
        SELECT COUNT(*)
        FROM time
        WHERE start_time IS NULL
    """