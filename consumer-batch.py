from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import mysql.connector
import time

# Create Spark session
spark = SparkSession.builder\
        .appName("tweets")\
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0")\
        .getOrCreate()


# Define Kafka consumer parameters
brokers = 'localhost:9092'
topics = ['covid19','coronavirus','lockdown']

# Define schema for the JSON data
schema = StructType([
    StructField("username", StringType(), True),
    StructField("tweet", StringType(), True),
    StructField("location", StringType(), True),
])

# Read data from Kafka in batch mode
df = spark \
    .read \
    .format("kafka") \
    .option("kafka.bootstrap.servers", brokers) \
    .option("subscribe", ",".join(topics)) \
    .option("startingOffsets", "earliest") \
    .option("endingOffsets", "latest") \
    .load()

# Parse the JSON data and create a DataFrame
df = df \
    .selectExpr("CAST(value AS STRING)") \
    .select(from_json("value", schema).alias("data")) \
    .select("data.*") \
    .withColumn("username", col("username"))

df = df.distinct().orderBy("username")
df.show()

df.createOrReplaceTempView("tweet")
result1 = spark.sql("SELECT username, location, COUNT(*) AS count FROM tweet GROUP BY username, location")
result2 = spark.sql("SELECT count(*) FROM tweet")

# Print the results
result1.show()
result2.show()

# df.show()
# Define MySQL database parameters
mysql_host = "localhost"
mysql_port = "3306"
mysql_user = "root"
mysql_password = "Dbt@2023"
mysql_database = "mydatabase"
mysql_table = "mytable"

# Create a MySQL connection
mysql_conn = mysql.connector.connect(
  host=mysql_host,
  port=mysql_port,
  user=mysql_user,
  password=mysql_password,
  database=mysql_database
)

# Write the DataFrame to MySQL
df.write \
  .format("jdbc") \
  .mode("append") \
  .option("url", f"jdbc:mysql://{mysql_host}:{mysql_port}/{mysql_database}") \
  .option("dbtable", mysql_table) \
  .option("user", mysql_user) \
  .option("password", mysql_password) \
  .save()

