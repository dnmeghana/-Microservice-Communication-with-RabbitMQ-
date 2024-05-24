from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, expr
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
# Create a SparkSession
spark = SparkSession.builder\
        .appName("tweets")\
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0")\
        .getOrCreate()

# Define the schema for the CSV data
schema = StructType([
    StructField("username", StringType(), True),
    StructField("tweet", StringType(), True),
    StructField("location", StringType(), True),
])
schema

brokers = 'localhost:9092'
# Define the Kafka topic to read from
topics = ['covid19','coronavirus','lockdown','London','USA','Others','Cannada']

# Define the Kafka parameters
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", brokers) \
    .option("subscribe", ",".join(topics)) \
    .option("startingOffsets", "earliest") \
    .load() \
    .selectExpr("CAST(value AS STRING)")

# Parse the JSON data and create a DataFrame
# Parse the JSON data and create a DataFrame
df = df \
    .selectExpr("CAST(value AS STRING)") \
    .select(from_json("value", schema).alias("data")) \
    .select("data.*") \
    .withColumn("username", col("username").cast("string"))

df = df.distinct()


# Aggregate the data by location and count the number of tweets per location
counts_df = df.groupby("location").count()

# Write the result DataFrame to the console
query_1 = counts_df.writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

# Print the tweets
tweets_query = df.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

query_1.awaitTermination()
tweets_query.awaitTermination()

