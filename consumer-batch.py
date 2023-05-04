from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import mysql.connector
import time
import datetime as dt

# Create Spark session
spark = SparkSession.builder\
        .appName("news")\
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1")\
        .config("spark.driver.extraClassPath", "/home/varshini/java/mysql-connector-j-8.0.33.jar") \
        .getOrCreate()

# Define Kafka consumer parameters
brokers = 'localhost:9092'
topics = ['technology', 'sports', 'politics', 'entertainment', 'world', 'automobile', 'science']

# Define schema for the JSON data
schema = StructType([
    StructField("id", StringType(), True),
    StructField("news_headline", StringType(), True),
    StructField("news_article", StringType(), True),
    StructField("news_category", StringType(), True)
    
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
    .withColumn("id", col("id").cast("int"))

df = df.distinct().orderBy("id")

# write df to one csv file  
# df.write.csv("news.csv", header=True)
df.createOrReplaceTempView("news")
start_time = dt.datetime.now()
result1 = spark.sql("SELECT news_category, COUNT(*) AS count FROM news GROUP BY news_category")
end_time = dt.datetime.now()

# result2 = spark.sql("SELECT news_headline, id FROM news WHERE news_category = 'sports'")
#result2 = spark.sql("SELECT count(*) FROM news where id=25")

#result3 = spark.sql("SELECT count(*) FROM news")

time_taken = (end_time - start_time).total_seconds()


# Define MySQL connection parameters
host = 'localhost'
port = '3306'
database = 'news'
user = 'root'
password = ''

# Define MySQL table schema
table_schema = 'CREATE TABLE IF NOT EXISTS news_count_batch (news_category VARCHAR(30), count INT)'

# Create MySQL connection and cursor
cnx = mysql.connector.connect(host=host, port=port, database=database, user=user, password=password)
cursor = cnx.cursor()

# Create MySQL table
cursor.execute(table_schema)

# Write DataFrame to MySQL table
result1.write.format('jdbc') \
    .options(url='jdbc:mysql://{0}:{1}/{2}'.format(host, port, database),
             driver='com.mysql.jdbc.Driver',
             dbtable='news_table',
             user=user,
             password=password) \
    .mode('overwrite') \
    .save()

# Close MySQL connection
cnx.close()

result1.show()
# Print results and time taken
print("Time taken for query: {} seconds. \n".format(time_taken))
# Stop Spark session
spark.stop()

# Print the results
#result2.show()
#result3.show()

# df.show()
