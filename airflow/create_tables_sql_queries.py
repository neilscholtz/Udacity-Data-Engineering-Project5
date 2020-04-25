# DROP TABLES
drop_artists_table = 'DROP TABLE IF EXISTS artists'
drop_users_table = 'DROP TABLE IF EXISTS users'
drop_time_table = 'DROP TABLE IF EXISTS time'
drop_songs_table = 'DROP TABLE IF EXISTS songs'
drop_songplays_table = 'DROP TABLE IF EXISTS songplays'
drop_staging_events_table = 'DROP TABLE IF EXISTS staging_events'
drop_staging_songs_table = 'DROP TABLE IF EXISTS staging_songs'

# CREATE TABLES
create_staging_events_table = '''
CREATE TABLE public.staging_events (
	artist varchar(256),
	auth varchar(256),
	firstName varchar(256),
	gender varchar(256),
	itemInSession int4,
	lastName varchar(256),
	length numeric(18,0),
	level varchar(256),
	location varchar(256),
	method varchar(256),
	page varchar(256),
	registration numeric(18,0),
	sessionId int4,
	song varchar(256),
	status int4,
	ts int8,
	userAgent varchar(256),
	userId int4
);
'''

create_staging_songs_table = '''
CREATE TABLE public.staging_songs (
	num_songs int4,
	artist_id varchar(256),
	artist_name varchar(256),
	artist_latitude numeric(18,0),
	artist_longitude numeric(18,0),
	artist_location varchar(256),
	song_id varchar(256),
	title varchar(256),
	duration numeric(18,0),
	year int4
);
'''

create_songplays_table = '''
CREATE TABLE public.songplays (
	songplay_id varchar(32) NOT NULL,
	start_time timestamp NOT NULL,
	userid int4 NOT NULL,
	level varchar(256),
	songid varchar(256),
	artistid varchar(256),
	sessionid int4,
	location varchar(256),
	user_agent varchar(256),
	CONSTRAINT songplays_pkey PRIMARY KEY (songplay_id)
);
'''

create_artists_table = '''
CREATE TABLE public.artists (
	artist_id varchar(256) NOT NULL,
	name varchar(256),
	location varchar(256),
	lattitude numeric(18,0),
	longitude numeric(18,0),
    CONSTRAINT artists_pkey PRIMARY KEY (artist_id)
);
'''

create_songs_table = '''
CREATE TABLE public.songs (
	song_id varchar(256) NOT NULL,
	title varchar(256),
	artist_id varchar(256),
	year int4,
	duration numeric(18,0),
	CONSTRAINT songs_pkey PRIMARY KEY (song_id)
);
'''

create_users_table = '''
CREATE TABLE public.users (
	userid int4 NOT NULL,
	first_name varchar(256),
	last_name varchar(256),
	gender varchar(256),
	level varchar(256),
	CONSTRAINT users_pkey PRIMARY KEY (userid)
);
'''

create_time_table = '''
CREATE TABLE public.time (
	start_time timestamp NOT NULL,
    hour int4,
    day int4,
    week int4,
    month varchar(256),
    year int4,
    weekday varchar(256),
    CONSTRAINT time_pkey PRIMARY KEY (start_time)
);
'''

# Lists of queries
drop_tables_list = [drop_artists_table, drop_users_table, drop_time_table, drop_songs_table, drop_songplays_table, drop_staging_events_table, drop_staging_songs_table]
create_tables_list = [create_staging_events_table, create_staging_songs_table, create_songplays_table, create_artists_table, create_users_table, create_time_table, create_songs_table]