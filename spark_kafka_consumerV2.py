from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp
from pyspark.sql.types import StructType, StringType, FloatType

# Create a Spark session and add ClickHouse JDBC driver
spark = SparkSession.builder \
    .appName("Kafka-Spark-Streaming") \
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,ru.yandex.clickhouse:clickhouse-jdbc:0.3.2") \
    .getOrCreate()

# Define the schema for Kafka messages
schema = StructType() \
    .add("symbol", StringType()) \
    .add("Datetime", StringType()) \
    .add("Open", FloatType()) \
    .add("High", FloatType()) \
    .add("Low", FloatType()) \
    .add("Close", FloatType()) \
    .add("Volume", FloatType()) \
    .add("Dividends", FloatType()) \
    .add("Stock_Splits", FloatType())

# Read data from Kafka
kafka_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "stock_prices") \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .load()


# Parse the raw Kafka messages (value column contains the message as string)
parsed_stream = kafka_stream.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# Convert Datetime from string to timestamp
parsed_stream = parsed_stream.withColumn(
    "Datetime",
    to_timestamp(col("Datetime"), "yyyy-MM-dd HH:mm:ssXXX")  # Parses datetime with timezone offset
)

# Filter out any null values after parsing
cleaned_stream = parsed_stream.filter(col("Datetime").isNotNull())

# Define ClickHouse JDBC properties
clickhouse_url = "jdbc:clickhouse://localhost:8123/stockDemoDB"
clickhouse_properties = {
    "driver": "ru.yandex.clickhouse.ClickHouseDriver",
    "user": "default",
    "password": ""
}

# Write the data to ClickHouse
query = cleaned_stream.writeStream \
    .outputMode("append") \
    .option("checkpointLocation", "/tmp/spark-checkpoints/stock_prices") \
    .foreachBatch(lambda df, batchId: df.write.jdbc(clickhouse_url, "stock_prices", mode="append", properties=clickhouse_properties)) \
    .start()

# Keep the stream running
query.awaitTermination()
