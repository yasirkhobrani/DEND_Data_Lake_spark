import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    
    '''
    process_song_data takes 3 parameters:
    * spark: created session from spark.
    * input_data: reads songs dataset from s3 bucket.
    * output_data: write extracted columns to parquet file
    '''
    # get filepath to song data file
    song_data = input_data+"song_data/*/*/*/*.json"
    
    # read song data file
    df = spark.read.json(song_data) 
    
    #create a view to use with SQL queries
    df.createOrReplaceTempView("songs_view")
    
    # extract columns to create songs table 
    songs_table = spark.sql("""
    
        SELECT DISTINCT song_id, title, artist_id, year,duration
        FROM songs_view 
        
        """)
    
     
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.mode('overwrite').partitionBy("year","artist_id").parquet(output_data+'song/') 

    # extract columns to create artists table
    # ARTIST TABLE
    artists_table = spark.sql("""
    
        SELECT DISTINCT artist_id, artist_name, artist_location, artist_latitude, artist_longitude
        FROM songs_view 
        
        """) 
    
    # write artists table to parquet files    
    artists_table.write.mode('overwrite').parquet(output_data+'artists_table/')


def process_log_data(spark, input_data, output_data):
    '''
    process_log_data takes 3 parameters:
    * spark: created session from spark.
    * input_data: reads logs dataset from s3 bucket.
    * output_data: write extracted columns to parquet file
    - joins songs and logs datasets.
    '''
    # get filepath to log data file
    log_data = input_data+'log-data/*.json'
    
    # read log data file
    df = spark.read.json(log_data) 
    
    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong') 
    
    #create a view to use with SQL queries
    df.createOrReplaceTempView("Logs_view")
    
    # extract columns for users table    
    users_table = spark.sql("""
        
        SELECT DISTINCT userId, firstName, lastName, gender, level
        FROM Logs_view
        
        """)
    
    # write users table to parquet files
    users_table.write.mode('overwrite').parquet(output_data+'users_table/')

    
    # extract columns to create time table
    time_table = spark.sql(""" 
    
        SELECT  DISTINCT temp.start_time, 
        hour( temp.start_time) as hour, 
        dayofmonth( temp.start_time) as day,
        weekofyear( temp.start_time) as week, 
        month( temp.start_time) as month, 
        year(temp.start_time) as year,
        dayofweek(temp.start_time) as weekday
        FROM (SELECT to_timestamp(ts/1000) as start_time
        FROM Logs_view ) temp
                             
        """)
    

    
    # time table to parquet files partitioned by year and month
    time_table.write.mode('overwrite').partitionBy("year", "month").parquet(output_data+'time/')

    # read in song data to use for songplays table
    song_df = spark.read.parquet(output_data+'song/') 

    # create songplays table by joining song and log datasets
    songplays_table = spark.sql(""" 
        SELECT  DISTINCT monotonically_increasing_id() as songplay_id,
        to_timestamp(ts/1000) as start_time,
        year(start_time) as year,
        month(start_time) as month,
        userId, song_id, artist_id, sessionId, location, userAgent
        FROM Logs_view 
        JOIN songs_view 
        ON  Logs_view.song = songs_view.title
        AND Logs_view.artist = songs_view.artist_name
        AND Logs_view.length = songs_view.duration
                                 
        """)


   
    # songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy("year","month").parquet(output_data+'songplays_table/')


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3://sparkify/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
