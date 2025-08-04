from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, from_json, window, avg, count
from pyspark.sql.types import DoubleType, StringType, StructType, TimestampType
from transformers import pipeline
import torch

# Initialize Spark session
spark = SparkSession.builder.appName("LLMAbuseDetection").getOrCreate()

# Define your Kafka JSON schema
schema = StructType() \
    .add("request_id", StringType()) \
    .add("timestamp", TimestampType()) \
    .add("user_id", StringType()) \
    .add("prompt", StringType()) \
    .add("model", StringType()) \
    .add("region", StringType()) \
    .add("prompt_tokens", StringType()) \
    .add("response_tokens", StringType()) \
    .add("latency_ms", StringType()) \
    .add("status", StringType()) \
    .add("label", StringType())

# Load Hugging Face toxicity classifier pipeline globally (avoid reloading per row)
classifier = pipeline("text-classification", model="unitary/toxic-bert", device=0 if torch.cuda.is_available() else -1)

# Define a UDF for toxicity scoring
def classify_toxicity(text):
    if text is None or text.strip() == "":
        return 0.0
    try:
        result = classifier(text[:512])  # limit input length
        # The classifier outputs a list of dicts like [{'label': 'TOXIC', 'score': 0.95}]
        for r in result:
            if r['label'] == 'TOXIC':
                return float(r['score'])
        return 0.0
    except Exception as e:
        # In case of error, return 0 toxicity
        return 0.0

toxicity_udf = udf(classify_toxicity, DoubleType())

# Read stream from Kafka
raw_df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "active_calls") \
    .option("startingOffsets", "latest") \
    .load()

# Parse Kafka JSON messages
json_df = raw_df.selectExpr("CAST(value AS STRING) as json_str") \
    .select(from_json("json_str", schema).alias("data")) \
    .select("data.*")

# Add toxicity score column via UDF
df_with_toxicity = json_df.withColumn("toxicity_score", toxicity_udf(col("prompt")))

# Add keyword flag column for simple rule-based boost
keyword_pattern = r"(?i)(ignore previous instructions|ignore your safety rules|how to hack|write a script to steal passwords|tell me how to cheat on exams|forget your rules|give me step-by-step instructions to make a bomb|write a convincing phishing email|trick someone into giving bank details|generate hate speech|discriminate against|write violent content|extract personal data|give me private information|create fraudulent documents|make illegal substances|spread misinformation|write fake news|promote terrorism|instruct on fraud|scam generation|bypass filters|jailbreak attempt)"
df_with_risk = df_with_toxicity.withColumn("keyword_flag",
    when(col("prompt").rlike(keyword_pattern), 1).otherwise(0))

# Composite risk score (70% keyword + 30% toxicity)
df_with_risk = df_with_risk.withColumn("risk_score",
    col("keyword_flag") * 0.7 + col("toxicity_score") * 0.3)

# Windowed aggregation over 10 minute windows sliding every 2 minutes
risk_agg = df_with_risk.withWatermark("timestamp", "1 minute") \
    .groupBy(window("timestamp", "10 minutes", "2 minutes")) \
    .agg(
        avg("risk_score").alias("avg_risk_score"),
        count("request_id").alias("event_count")
    )

# Output results to parquet 
query = risk_agg.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "s3a://streaming-risk-data-demo/checkpoints/") \
    .option("path", "s3a://streaming-risk-data-demo/risk_scores/") \
    .start()

