from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, udf, struct, to_json
from pyspark.sql.types import StructType, StringType, DoubleType
from pyspark.ml import PipelineModel
from pyspark.sql.streaming import DataStreamWriter
from pymongo import MongoClient
import json
import os
from datetime import datetime
from pyspark.sql.functions import lit

# --- Spark session ---
spark = SparkSession.builder \
    .appName("RealTimeSentimentConsumer") \
    .master("spark://spark-master:7077") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# --- Load model ---
model_path = "/app/model/best_model/SentimentModel"
model = PipelineModel.load(model_path)

# --- Kafka config ---
kafka_bootstrap = os.getenv("KAFKA_BROKER", "kafka:9092")
input_topic = os.getenv("KAFKA_INPUT_TOPIC", "amazon-reviews1")
output_topic = os.getenv("KAFKA_OUTPUT_TOPIC", "sentiment-results")

# --- Mongo config ---
mongo_host = os.getenv("MONGO_HOST", "mongodb")
mongo_port = int(os.getenv("MONGO_PORT", 27017))

# --- Define input schema ---
schema = StructType() \
    .add("reviewerID", StringType()) \
    .add("lemmatized_text", StringType()) \
    .add("overall", DoubleType()) \
    .add("label", DoubleType())

# --- Read from Kafka ---
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap) \
    .option("subscribe", input_topic) \
    .option("startingOffsets", "latest") \
    .load()

df_parsed = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# --- Predict using model ---
predictions = model.transform(df_parsed)

# --- Final format ---
from pyspark.sql.functions import current_timestamp

result_df = predictions.select(
    col("reviewerID"),
    col("lemmatized_text"),
    col("prediction").cast("int").alias("predicted_label"),
    current_timestamp().alias("ingestion_time")  # Ajout du timestamp
)

# --- Save to MongoDB ---
def save_to_mongo(batch_df, batch_id):
    print(f"\n🔄 [Batch {batch_id}] --- Incoming Records ---")
    
    # Collect and print predictions
    records = batch_df.select("reviewerID", "lemmatized_text", "predicted_label").collect()
    for r in records:
        print(f"🗣 Reviewer: {r['reviewerID']} | 📝 Text: {r['lemmatized_text'][:60]}... | 🎯 Prediction: {r['predicted_label']}")

    # Save to Mongo
    if records:
        client = MongoClient(mongo_host, mongo_port)
        db = client.reviews
        db.predictions.insert_many([row.asDict() for row in records])
        client.close()
        print(f"✅ Inserted {len(records)} records into MongoDB.")
    else:
        print("⚠️ No records to insert.")

# --- Send to Kafka ---
kafka_output = result_df.selectExpr("to_json(struct(*)) AS value")

query_kafka = kafka_output.writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap) \
    .option("topic", output_topic) \
    .option("checkpointLocation", "/tmp/checkpoint-kafka") \
    .outputMode("append") \
    .start()

# --- Send to MongoDB ---
query_mongo = result_df.writeStream \
    .foreachBatch(save_to_mongo) \
    .outputMode("append") \
    .option("checkpointLocation", "/tmp/checkpoint-mongo") \
    .start()

# --- Await termination ---
query_kafka.awaitTermination()
query_mongo.awaitTermination()
