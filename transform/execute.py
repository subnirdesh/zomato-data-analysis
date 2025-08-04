import sys,os,time
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__),'..')))
from utility.utility import setup_logging,format_time



def create_spark_session():
    return (SparkSession.builder
            .appName("SpotifyDataTransform")
            .config("spark.driver.memory", "2g")
            .config("spark.executor.memory", "4g")
            .getOrCreate()
    )

def load_and_clean(spark, input_dir,output_dir ):
    """Stage 1: Load and Clean Data"""

    # Define schema
    artists_schema = T.StructType([
        T.StructField("id", T.StringType(), True),
        T.StructField("followers", T.StringType(), True),  # Read as string first
        T.StructField("genres", T.StringType(), True),
        T.StructField("name", T.StringType(), True),        # name comes fourth
        T.StructField("popularity", T.IntegerType(), True)
    ])

    recommendations_schema = T.StructType([
        T.StructField("id", T.StringType(), False),
        T.StructField("related_ids", T.ArrayType(T.StringType()), True)
    ])

    tracks_schema = T.StructType([
        T.StructField("id", T.StringType(), False),
        T.StructField("name", T.StringType(), True),
        T.StructField("popularity", T.IntegerType(), True),
        T.StructField("duration_ms", T.IntegerType(), True),
        T.StructField("explicit", T.IntegerType(), True),
        T.StructField("artists", T.StringType(), True),
        T.StructField("id_artists", T.StringType(), True),
        T.StructField("release_date", T.StringType(), True),
        T.StructField("danceability", T.FloatType(), True),
        T.StructField("energy", T.FloatType(), True),
        T.StructField("key", T.IntegerType(), True),
        T.StructField("loudness", T.FloatType(), True),
        T.StructField("mode", T.IntegerType(), True),
        T.StructField("speechiness", T.FloatType(), True),
        T.StructField("acousticness", T.FloatType(), True),
        T.StructField("instrumentalness", T.FloatType(), True),
        T.StructField("liveness", T.FloatType(), True),
        T.StructField("valence", T.FloatType(), True),
        T.StructField("tempo", T.FloatType(), True),
        T.StructField("time_signature", T.IntegerType(), True)
    ])


    artists_df = spark.read.schema(artists_schema).csv(os.path.join(input_dir, "artists.csv"), header=True)
    # Clean followers column
    artists_df = artists_df.withColumn(
    "followers",
    F.regexp_replace("followers", ",", "").cast(T.FloatType())
    )
    recommendations_df = spark.read.schema(recommendations_schema).json(os.path.join(input_dir, "fixed_da.json"))
    tracks_df = spark.read.schema(tracks_schema).csv(os.path.join(input_dir, "tracks.csv"), header=True)

    artists_df = artists_df.dropDuplicates(["id"]).filter(F.col("id").isNotNull())
    recommendations_df = recommendations_df.dropDuplicates(["id"]).filter(F.col("id").isNotNull())
    tracks_df = tracks_df.dropDuplicates(["id"]).filter(F.col("id").isNotNull())

    artists_df.write.mode("overwrite").parquet(os.path.join(output_dir, "stage1", "artists"))
    recommendations_df.write.mode("overwrite").parquet(os.path.join(output_dir, "stage1", "recommendations"))
    tracks_df.write.mode("overwrite").parquet(os.path.join(output_dir, "stage1", "tracks"))

    logger.info("Stage 1: Data loaded and cleaned successfully.")
    return artists_df, recommendations_df, tracks_df    


from pyspark.sql import functions as F
import os

def create_master_table(artists_df, recommendations_df, tracks_df, output_dir):
    """Stage 2: Create Master Tables by joining artists and tracks data"""
    
    # 1. Convert string to array
    tracks_with_array = tracks_df.withColumn(
        "id_artists_array", 
        F.from_json(F.col("id_artists"), F.ArrayType(F.StringType()))
    )
    
    # 2. EXPLODE the array to create multiple rows
    tracks_exploded = tracks_with_array.select(
        F.col("id").alias("track_id"),
        F.col("name").alias("track_name"),
        F.col("popularity").alias("track_popularity"),
        F.col("danceability"),
        F.col("energy"),
        F.col("tempo"),
        F.explode(F.col("id_artists_array")).alias("artist_id")  # individual artist IDs
    )
    logger.debug(f"After exploding tracks: {tracks_exploded.count()} rows")
    
    # 3. Trim and lowercase artist IDs to avoid join mismatches
    tracks_exploded = tracks_exploded.withColumn("artist_id", F.lower(F.trim("artist_id")))
    artists_df = artists_df.withColumn("id", F.lower(F.trim("id")))

    # 4. Join on the EXPLODED artist_id
    master_df = tracks_exploded.join(
        artists_df,
        tracks_exploded.artist_id == artists_df.id,
        "left"
    ).select(
        tracks_exploded.track_id,
        tracks_exploded.track_name,
        tracks_exploded.track_popularity,
        artists_df.id.alias("artist_id"),
        artists_df.name.alias("artist_name"),
        artists_df.followers,
        artists_df.genres,
        artists_df.popularity.alias("artist_popularity"),
        tracks_exploded.danceability,
        tracks_exploded.energy,
        tracks_exploded.tempo
    )
    logger.debug(f"After joining with artists: {master_df.count()} rows")

    # 5. Debug unmatched artists
    unmatched_artists = tracks_exploded.join(
        artists_df,
        tracks_exploded.artist_id == artists_df.id,
        how='left_anti'
    ).select("artist_id").distinct()

    logger.debug("Unmatched artist IDs (missing from artists data):")
    unmatched_artists.show(20, truncate=False)

    # 6. Optional: filter out rows where artist info is missing
    master_df_filtered = master_df.filter(master_df.artist_id.isNotNull())

    # 7. Join with recommendations
    final_df = master_df_filtered.join(
        recommendations_df,
        master_df_filtered.artist_id == recommendations_df.id,
        "left"
    ).select(
        master_df_filtered.track_id,
        master_df_filtered.track_name,
        master_df_filtered.track_popularity,
        master_df_filtered.artist_id,
        master_df_filtered.artist_name,
        master_df_filtered.followers,
        master_df_filtered.genres,
        master_df_filtered.artist_popularity,
        master_df_filtered.danceability,
        master_df_filtered.energy,
        master_df_filtered.tempo,
        recommendations_df.related_ids
    )
    logger.debug(f"Final master table rows: {final_df.count()}")

    # Save parquet
    final_df.write.mode("overwrite").parquet(os.path.join(output_dir, "stage2", "master_table"))
    logger.info("Stage 2: Master table saved")



    
def create_query_table(output_dir, artists_df, recommendations_df, tracks_df):
    """Stage 3: Create query-optimized tables."""

    recommendations_exploded = recommendations_df.withColumn("related_id", F.explode("related_ids")) \
                                                .select("id", "related_id")
    recommendations_exploded.write.mode("overwrite").parquet(os.path.join(output_dir, "stage3", "recommendations_exploded"))

    tracks_exploded = tracks_df.withColumn("id_artists_array", F.from_json(F.col("id_artists"), T.ArrayType(T.StringType()))) \
                                .withColumn("id_artists", F.explode("id_artists_array")) \
                                .select("id", "id_artists")
    tracks_exploded.write.mode("overwrite").parquet(os.path.join(output_dir, "stage3", "artist_track"))

    tracks_metadata = tracks_df.select(
    "id", "name", "popularity", "duration_ms", "danceability", "energy", "tempo"
)
    tracks_metadata.write.mode("overwrite").parquet(os.path.join(output_dir, "stage3", "track_metadata"))

    artists_metadata = artists_df.select("id", "name", "followers", "popularity")
    artists_metadata.write.mode("overwrite").parquet(os.path.join(output_dir, "stage3", "artist_metadata"))

    logger.info("Stage 3: Query-optimized tables saved")



if __name__ == "__main__":

    logger=setup_logging("transform.log")
    start=time.time()


    if len(sys.argv) != 3:
        logger.error("Usage: python script.py <input_dir> <output_dir>")
        sys.exit(1)

    input_dir = sys.argv[1]
    output_dir = sys.argv[2]

    spark = create_spark_session()

    artists_df, recommendations_df, tracks_df = load_and_clean(spark, input_dir, output_dir)
    # ADD DEBUG HERE
    logger.info("=== DEBUGGING BEFORE MASTER TABLE CREATION ===")
    logger.debug(f"Artists count: {artists_df.count()}")
    logger.debug(f"Tracks count: {tracks_df.count()}")
    tracks_df.select("id", "id_artists").show(5, truncate=False)
    create_master_table( artists_df, recommendations_df, tracks_df,output_dir)
    create_query_table(output_dir, artists_df, recommendations_df, tracks_df)

    end=time.time()
    logger.info("Transformation pipeline completed")
    logger.info(f"Total time taken{ format_time(end-start)}")
    









