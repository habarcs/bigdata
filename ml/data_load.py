from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("OrderPredictionPipeline") \
    .getOrCreate()

# Load data from PostgreSQL
jdbc_url = "jdbc:postgresql://<host>:<port>/<database>"
properties = {
    "user": "<username>",
    "password": "<password>",
    "driver": "org.postgresql.Driver"
}

df = spark.read.jdbc(jdbc_url, table="orders", properties=properties)

# Alternatively, read directly from Kafka if needed
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "orders") \
    .load()
