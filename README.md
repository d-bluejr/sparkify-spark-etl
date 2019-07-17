Summary<br>
The purpose of this project is to load and transform JSON log files into a star schema for storing Sparkify's app data. This star schema will then be saved back to files. This is all accomplished via Spark. <br>

Schema <br>
Name: sparkify <br>
Tables: <br>
    artists (artist_id, name, location, latittude, longitude) - the artist data of the songs in the app <br>
    songs (song_id, title, artist_id, year, duration) - the song data of the songs in the app <br>
    songplays (songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent) - the play data for a song played by a user on the app <br>
    time (start_time, hour, day, week, month, year, weekday) - the song timestamp data broken into different units of measurement <br>
    users (user_id, first_name, last_name, gender, level) - the user data for the app <br>

How to Run<br>
Run the following files in the below order to create and populate the database:<br>
1. etl.py<br>

Files<br>
dl.cfg - The config file used to store the AWS key and sceret.<br>
etl.py - The script used to load the song JSON file and app log file data. Provides the control logic for the ETL process.<br>

ETL Process Explanation<br>
The files are loaded via the following process:<br>
1. The song files are loaded from S3, transformed into the song and artist tables, and the tables are saved to files. <br>
2. The log files are loaded from S3, transformed into the user, time, and songplay tables, and the tables are saved to files.<br>
