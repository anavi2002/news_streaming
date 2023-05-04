from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, expr
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
# Create a SparkSession
spark = SparkSession.builder\
        .appName("news")\
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0")\
        .config("spark.driver.extraClassPath", "/home/varshini/java/mysql-connector-j-8.0.33.jar") \
        .getOrCreate()

# Define the schema for the CSV data
schema = StructType([
    StructField("id", StringType(), True),
    StructField("news_headline", StringType(), True),
    StructField("news_article", StringType(), True),
    StructField("news_category", StringType(), True)
])
schema

brokers = 'localhost:9092'
# Define the Kafka topic to read from
topics = ['sports','technology','politics']

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
df = df \
    .selectExpr("CAST(value AS STRING)") \
    .select(from_json("value", schema).alias("data")) \
    .select("data.*") \
    .withColumn("id", col("id").cast("int"))

df = df.distinct()

# Define the JDBC connection parameters
url = "jdbc:mysql://localhost:3306/news"
table_name = "news_table"
properties = {
    "user": "root",
    "password": "",
    "driver": "com.mysql.cj.jdbc.Driver"
}

# Write the result DataFrame to the console
counts_df = df.groupby("news_category").count()
query = counts_df.writeStream \
    .outputMode("complete") \
    .option("truncate", True) \
    .format("console") \
    .foreachBatch(lambda batch_df, batch_id: batch_df.write.jdbc(url=url, table=table_name, mode="append", properties=properties))\
    .start()
    
query_result = counts_df.writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

#spark.sparkContext.setLogLevel("INFO")


query.awaitTermination()
