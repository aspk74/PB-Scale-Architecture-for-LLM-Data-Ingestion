from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, TimestampType

# Define Kafka JSON schema (match your data)
schema = StructType() \
    .add("call_id", StringType()) \
    .add("caller_number", StringType()) \
    .add("timestamp", TimestampType()) \
    # add other fields as needed

spark = SparkSession.builder.appName("TestRawWrite").getOrCreate()

raw_df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "active_calls") \
    .option("startingOffsets", "latest") \
    .load()

json_df = raw_df.selectExpr("CAST(value AS STRING) as json_str") \
    .select(from_json("json_str", schema).alias("data")) \
    .select("data.*")

query = json_df.writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", "./storage/raw_data_parquet") \
    .option("checkpointLocation", "./checkpoints/raw_data_checkpoint") \
    .trigger(processingTime="10 seconds") \
    .start()

query.awaitTermination()
