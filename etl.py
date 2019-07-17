import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql import types as T


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['CONFIG']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['CONFIG']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """Creates the Spark session
    
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """Processes the song data JSON files
    
    Keyword arguments:
    spark -- the Spark session 
    input_data -- the file input directory
    output_data -- the file output directory
    """
    
    # get filepath to song data file
    song_data = os.path.join(input_data, "song_data/*/*/*/*.json")
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select(["song_id", "title", "artist_id", "year", "duration"]).drop_duplicates(subset=["song_id"])
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.mode("overwrite").partitionBy("year","artist_id").parquet(os.path.join(output_data, "songs"))

    # extract columns to create artists table
    artists_table = df.select(col("artist_id"), col("artist_name").alias("name"), col("artist_location").alias("location"), col("artist_latitude").alias("latitude"),  col("artist_longitude").alias("longitude")).drop_duplicates(subset=["artist_id"])
    
    # write artists table to parquet files
    artists_table.write.mode("overwrite").parquet(os.path.join(output_data, "artists"))


def process_log_data(spark, input_data, output_data):
    """Processes the log data JSON files
    
    Keyword arguments:
    spark -- the Spark session 
    input_data -- the file input directory
    output_data -- the file output directory
    """
    
    # get filepath to log data file
    log_data = os.path.join(input_data, "log_data/*/*/*.json")

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter(df.page == "NextSong")

    # extract columns for users table    
    user_table = df.select(col("userId").alias("user_id"), col("firstName").alias("first_name"), col("lastName").alias("last_name"), "gender", "level").drop_duplicates(subset=["user_id"])
    
    # write users table to parquet files
    user_table.write.mode("overwrite").parquet(os.path.join(output_data, "users"))

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: datetime.fromtimestamp(x/1000.0),T.TimestampType())
    df = df.withColumn("timestamp", get_timestamp(df.ts))
    
    # extract columns to create time table
    time_table = df.select(col("ts").alias("start_time"), hour(col("timestamp")).alias("hour"), dayofmonth(col("timestamp")).alias("day"), weekofyear(col("timestamp")).alias("week"), month(col("timestamp")).alias("month"), year(col("timestamp")).alias("year"), date_format('timestamp', 'u').alias("weekday")).drop_duplicates(subset=["start_time"])
    
    # write time table to parquet files partitioned by year and month
    time_table.write.mode("overwrite").partitionBy("year","month").parquet(os.path.join(output_data, "times"))

    # read in song data to use for songplays table
    song_df = spark.read.parquet(os.path.join(output_data, "songs"))

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = df.join(song_df, df["song"] == song_df["title"], "left")
    songplays_table = songplays_table.join(time_table, songplays_table["ts"] == time_table["start_time"], "left").select(col("start_time"), col("userId").alias("user_id"), col("level"), col("song_id"), col("artist_id"), col("sessionId").alias("session_id"), col("location"), col("userAgent").alias("user_agent"), time_table["year"], col("month"))
    songplays_table = songplays_table.withColumn("songplay_id", monotonically_increasing_id())
    
    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.mode("overwrite").partitionBy("year","month").parquet(os.path.join(output_data, "songplays"))

    
def main():
    #Create the spark session
    spark = create_spark_session()
    #Set the input and output directories
    input_data = "s3a://udacity-dend/"
    output_data = "output/"
    
    #Processing the song data
    process_song_data(spark, input_data, output_data)   
    #Processing the log data
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
