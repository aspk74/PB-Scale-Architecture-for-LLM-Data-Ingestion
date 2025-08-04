from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, expr
from pyspark.sql.types import StructType, StringType, FloatType, TimestampType

# Define the schema of the log JSON
log_schema = StructType() \
    .add("timestamp", TimestampType()) \
    .add("user_id", StringType()) \
    .add("prompt", StringType()) \
    .add("response", StringType()) \
    .add("attack_type", StringType()) \
    .add("toxicity_score", FloatType())

# Start Spark session
spark = SparkSession.builder \
    .appName("LLMAbuseMonitor") \
    .getOrCreate()

# Read from Kafka
df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "llm_logs") \
    .option("startingOffsets", "latest") \
    .load()

# Convert the binary value to string, parse JSON
df_parsed = df_raw.selectExpr("CAST(value AS STRING) as json") \
    .withColumn("data", from_json(col("json"), log_schema)) \
    .select("data.*")

# Example analysis: count of attacks per attack type
attack_counts = df_parsed.groupBy("attack_type").count()

# Output to console (for now)
output_path = "./storage/attack_counts_parquet"

query = attack_counts.writeStream \
    .outputMode("complete") \
    .format("parquet") \
    .option("path", output_path) \
    .option("checkpointLocation", "./checkpoints/attack_counts") \
    .trigger(processingTime="10 seconds") \
    .start()

# Await termination of the stream
query.awaitTermination()
