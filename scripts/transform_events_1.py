from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_date, when, regexp_replace, 
    current_date, datediff, split, explode
)

def transform_events(input_path):
    """Transform events data using Spark"""
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("EventsTransformation") \
        .getOrCreate()
    
    # Read CSV file
    df = spark.read.csv(input_path, header=True, inferSchema=True)
    
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
    
    # Save transformed data
    output_path = input_path.replace('raw', 'processed')
    transformed_df.write.csv(output_path, header=True, mode="overwrite")
    
    # Generate some basic analytics
    print("\nEvent Statistics:")
    print(f"Total Events: {transformed_df.count()}")
    print(f"Unique Venues: {transformed_df.select('venue_name').distinct().count()}")
    print("\nTop Rated Venues:")
    transformed_df.select("venue_name", "venue_rating", "venue_reviews") \
        .distinct() \
        .orderBy(col("venue_rating").desc()) \
        .show(5, False)
    
    # Show upcoming events
    print("\nUpcoming Events (Next 7 Days):")
    transformed_df.where(col("days_until_event").between(0, 7)) \
        .select("title", "date", "venue_name") \
        .show(5, False)
    
    spark.stop()
    return output_path

if __name__ == "__main__":
    # input_path = "/opt/airflow/data/raw/events_austin_20240112.csv"
    input_path = "data/raw/events_austin_20250112.csv"
    transform_events(input_path)