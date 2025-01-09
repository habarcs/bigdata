from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.ml import PipelineModel
from pyspark.ml.feature import VectorAssembler
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("Kafka-Spark-ML-Pipeline") \
    .getOrCreate()

# Kafka Configuration
kafka_bootstrap_servers = "localhost:9092"
input_topic = "orders"
output_topic = "predictions"

# Define Schema for Input Data
schema = StructType([
    StructField("transaction_type", StringType(), True),
    StructField("real_shipping_days", FloatType(), True),
    StructField("scheduled_shipping_days", FloatType(), True),
    StructField("delivery_status", StringType(), True),
    StructField("late_risk", IntegerType(), True),
    StructField("order_date", StringType(), True),
    StructField("order_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("item_quantity", IntegerType(), True),
    StructField("status", StringType(), True),
    StructField("shipping_date", StringType(), True),
    StructField("shipping_mode", StringType(), True),
    StructField("customer_id", StringType(), True),
])

# Load Pre-trained Model
model = PipelineModel.load("shipping_days_model")

# Read from Kafka
raw_stream = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", input_topic) \
    .option("startingOffsets", "latest") \
    .load()

# Parse JSON from Kafka
orders_stream = raw_stream.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# Feature Engineering
assembler = VectorAssembler(
    inputCols=["scheduled_shipping_days", "late_risk", "item_quantity"],
    outputCol="features"
)

orders_with_features = assembler.transform(orders_stream)

# Make Predictions
predictions = model.transform(orders_with_features) \
    .select("order_id", "prediction")

# Write Predictions Back to Kafka
predictions.selectExpr("CAST(order_id AS STRING) AS key", "CAST(prediction AS STRING) AS value") \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("topic", output_topic) \
    .option("checkpointLocation", "/tmp/kafka-spark-checkpoint") \
    .start() \
    .awaitTermination()
