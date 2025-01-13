from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_date, when, regexp_replace, 
    current_date, datediff, split, explode
)
import os

def transform_events(input_path):
    """Transform events data using Spark"""
    # Initialize Spark session with proper configuration
    spark = SparkSession.builder \
        .appName("EventsTransformation") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4") \
        .config("spark.driver.memory", "2g") \
        .config("spark.executor.memory", "2g") \
        .config("spark.python.worker.memory", "1g") \
        .config("spark.driver.extraJavaOptions", "-XX:+UseG1GC") \
        .config("spark.executor.extraJavaOptions", "-XX:+UseG1GC") \
        .config("spark.sql.session.timeZone", "UTC") \
        .master("local[*]") \
        .getOrCreate()

    try:
        # Read CSV file
        print(f"Reading input file: {input_path}")
        df = spark.read.csv(input_path, header=True, inferSchema=True)
        
        print(f"Total records read: {df.count()}")
        
        # Clean and transform data
        transformed_df = df.select(
            col("title"),
            col("date"),
            col("when"),
            col("venue_name"),
            when(col("venue_rating").isNull(), 0.0)
            .otherwise(col("venue_rating")).alias("venue_rating"),
            when(col("venue_reviews").isNull(), 0)
            .otherwise(col("venue_reviews")).alias("venue_reviews"),
            col("address"),
            when(col("description").isNull(), "No description available")
            .otherwise(col("description")).alias("description"),
            col("link"),
            col("ticket_info"),
            col("fetch_date")
        )
        
        # Clean up description text
        transformed_df = transformed_df.withColumn(
            "description",
            regexp_replace(col("description"), "\\.\\.\\.$", "")
        )
        
        # Add some analytics
        transformed_df = transformed_df.withColumn(
            "days_until_event",
            when(col("date").isNotNull(),
                 datediff(to_date(col("date"), "MMM d"), current_date()))
            .otherwise(None)
        )
        
        # Generate some basic analytics
        print("\nEvent Statistics:")
        total_events = transformed_df.count()
        print(f"Total Events: {total_events}")
        
        unique_venues = transformed_df.select('venue_name').distinct().count()
        print(f"Unique Venues: {unique_venues}")
        
        print("\nTop Rated Venues:")
        transformed_df.select("venue_name", "venue_rating", "venue_reviews") \
            .distinct() \
            .orderBy(col("venue_rating").desc()) \
            .show(5, False)
        
        # Save transformed data
        output_path = input_path.replace('raw', 'processed')
        print(f"Saving transformed data to: {output_path}")
        transformed_df.write.csv(output_path, header=True, mode="overwrite")
        
        return output_path
        
    except Exception as e:
        print(f"Error in transform_events: {str(e)}")
        raise
    finally:
        # Always stop SparkSession
        if spark:
            spark.stop()

if __name__ == "__main__":
    # input_path = "/opt/airflow/data/raw/events_austin_20250112.csv"
    input_path = "data/raw/events_austin_20250112.csv"
    transform_events(input_path)