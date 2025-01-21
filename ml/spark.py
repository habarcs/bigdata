import time
import warnings
import os
warnings.filterwarnings("ignore")
import logging

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, IntegerType
from pyspark.sql.functions import (
    col,
    from_json,
    to_timestamp,
    when,
    floor,
    unix_timestamp,
    avg,
    sum as _sum,
    to_date,
)
import pyspark.sql.functions as F
from prophet import Prophet
import pandas as pd

pd.DataFrame.iteritems = pd.DataFrame.items

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class SparkDataProcessAndForecast:
    def __init__(self, retries=5, delay=5):
        """
        Initialize the Spark session and set class-level configurations.
        """

        # Create Spark Session
        # https://jdbc.postgresql.org/download/postgresql-42.7.4.jar - source link for spark connector
        logger.info("Initializing Spark session...")

        self.spark = (
            SparkSession.builder.appName("KafkaSparkConnector")
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.4") \
            .config("spark.jars", "./postgresql-42.7.4.jar") \
            .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
            .getOrCreate()
        )

        # Store JDBC properties
        self.jdbc_url = "jdbc:postgresql://sql-database:5432/postgres"  # sql-database - service name from Docker Compose
        self.jdbc_properties = {
            "user": "postgres",
            "password": "supersecret",
            "driver": "org.postgresql.Driver",
        }

        # Kafka schema
        self.kafka_schema = (
            StructType()
            .add("transaction_type", StringType())
            .add("real_shipping_days", IntegerType())
            .add("scheduled_shipping_days", IntegerType())
            .add("delivery_status", StringType())
            .add("late_risk", IntegerType())
            .add("order_date", StringType())
            .add("order_id", IntegerType())
            .add("product_id", IntegerType())
            .add("item_quantity", IntegerType())
            .add("status", StringType())
            .add("shipping_data", StringType())
            .add("shipping_mode", StringType())
            .add("customer_id", IntegerType())
            .add("retailer_id", IntegerType())
        )

        # Retry logic configs
        self.retries = retries
        self.delay = delay

    def read_kafka_stream(self):
        """
        Read a streaming DataFrame from Kafka.
        """
        logger.info("Reading data from Kafka stream...")

        kafka_stream = (
            self.spark.readStream.format("kafka")  # 
            .option("kafka.bootstrap.servers", "kafka:9092")  # kafka - service name from Docker Compose
            .option("subscribe", "orders")
            .option("startingOffsets", "earliest")
            .load()
        )

        # Parse the JSON from Kafka
        parsed_stream = (
            kafka_stream.selectExpr("CAST(value AS STRING)")
            .select(from_json(col("value"), self.kafka_schema).alias("data"))
            .select("data.*")
        )
        return parsed_stream

    def connect_to_postgresql_with_retry(self):
        """
        Connect to Postgres with retry logic. Returns customer, product, and retailer dataframes.
        """
        logger.info("Connecting to Postgres with retries...")

        for attempt in range(self.retries):
            try:
                customer_data = self.spark.read.jdbc(
                    self.jdbc_url, "customers", properties=self.jdbc_properties
                )
                product_data = self.spark.read.jdbc(
                    self.jdbc_url, "products", properties=self.jdbc_properties
                )
                retailer_data = self.spark.read.jdbc(
                    self.jdbc_url, "retailers", properties=self.jdbc_properties
                )
                logger.info("Connection to Postgres successful.")
                return customer_data, product_data, retailer_data
            except Exception as e:
                logger.error(f"Attempt {attempt + 1} failed: {e}")
                if attempt < self.retries - 1:
                    time.sleep(self.delay)
                else:
                    raise

    def enrich_data(self, parsed_stream, customer_data, product_data, retailer_data):
        """
        Join the Kafka stream with Postgres tables and enrich with new columns.
        """
        # Enrich Data from the PostgreSQL tables
        enriched_stream = (
            parsed_stream.join(customer_data, "customer_id", "left")
            .join(product_data, "product_id", "left")
            .join(retailer_data, "retailer_id", "left")
            .withColumn("order_timestamp", to_timestamp(col("order_date"), "yyyy/MM/dd HH:mm"))
            .withColumn(
                "shipping_delay", col("real_shipping_days") - col("scheduled_shipping_days")
            )
            .withColumn("on_time", when(col("shipping_delay") <= 0, 1).otherwise(0))
        )

        return enriched_stream

    def wait_for_data_and_collect(self, enriched_stream, start_date=None, end_date=None, product_ids=None, retailer_ids=None):
        """
        Write enriched stream data to an in-memory table, wait until it finishes,
        then return the final Spark DataFrame with optional filtering by product_ids and retailer_ids.
        """
        self.spark.sql("DROP TABLE IF EXISTS enriched_data")

        memory_query = (
            enriched_stream.writeStream.format("memory")
            .queryName("enriched_data")
            .trigger(once=True)  # Process all available data then stop
            .start()
        )

        # Wait until all data has been processed
        memory_query.awaitTermination()
        final_enriched_df = self.spark.sql("SELECT * FROM enriched_data")

        # Apply date filtering if provided
        if start_date and end_date:
            final_enriched_df = final_enriched_df.filter(
                (col("order_timestamp") >= start_date) & (col("order_timestamp") <= end_date)
            )

        # Apply product_ids filtering if provided
        if product_ids and isinstance(product_ids, list):
            final_enriched_df = final_enriched_df.filter(col("product_id").isin(product_ids))

        # Apply retailer_ids filtering if provided
        if retailer_ids and isinstance(retailer_ids, list):
            final_enriched_df = final_enriched_df.filter(col("retailer_id").isin(retailer_ids))

        return final_enriched_df


    def preprocessing(self, df):
        """
        Perform additional transformations on the final enriched DataFrame (e.g. timestamps, shipping duration).
        """
        logger.info("Starting the preprocessing...")

        df_transformed = df.withColumn(
            "order_date", to_timestamp(col("order_date"), "yyyy/MM/dd HH:mm")
        ).withColumn(
            "shipping_data", to_timestamp(col("shipping_data"), "yyyy/MM/dd HH:mm")
        ).withColumn(
            "shipping_duration_hours",
            floor((unix_timestamp(col("shipping_data")) - unix_timestamp(col("order_date"))) / 3600)
        )
        # Aggregate data
        aggregated_data = (
            df_transformed.groupBy(to_date("order_timestamp").alias("ds"), "retailer_id", "product_id")
            .agg(
                _sum("item_quantity").alias("y"),
                avg("real_shipping_days").alias("real_shipping_days"),
                avg("scheduled_shipping_days").alias("scheduled_shipping_days"),
                avg("late_risk").alias("late_risk"),
                avg("product_price").alias("product_price"),
                avg("shipping_delay").alias("shipping_delay"),
                avg("on_time").alias("on_time"),
                avg("shipping_duration_hours").alias("shipping_duration_hours"),
            )
            .orderBy("ds", "retailer_id", "product_id")
        )
        logger.info("Finished the preprocessing...")

        return aggregated_data.toPandas()

    def prophet_forecast(self, final_df_pandas, days=7):
        """
        Train Prophet models for each retailer_id/product_id combination
        and generate predictions for future dates only.
        """

        # Train Prophet models
        models = {}
        retailer_product_groups = final_df_pandas.groupby(["retailer_id", "product_id"])

        for (retailer_id, product_id), group in retailer_product_groups:
            df = group[["ds", "y"]]  # Use only the required columns
            df = df.dropna().sort_values("ds")  # Ensure data is sorted by date

            if df.shape[0] < 2:
                print(f"Skipping retailer_id: {retailer_id}, product_id: {product_id} (insufficient data).")
                continue

            # Initialize Prophet model
            model = Prophet(yearly_seasonality=False, weekly_seasonality=False, daily_seasonality=False)
            model.add_seasonality(name='monthly', period=30.5, fourier_order=5)
            model.fit(df)

            models[(retailer_id, product_id)] = model

        # Generate predictions for future dates only
        future_predictions = []
        for (retailer_id, product_id), model in models.items():
            # Generate future dates
            future = model.make_future_dataframe(periods=days)
            
            # Filter for future dates only
            last_date = model.history["ds"].max()  # Last date in the historical data
            future = future[future["ds"] > last_date]

            # Make predictions
            forecast = model.predict(future)

            # Add metadata
            forecast["retailer_id"] = retailer_id
            forecast["product_id"] = product_id

            future_predictions.append(forecast)

        predictions_spark_df = None
        # Combine all predictions into a single DataFrame
        if future_predictions:
            predictions_df = pd.concat(future_predictions, ignore_index=True)
            predictions_spark_df = self.spark.createDataFrame(predictions_df.astype(str))
            predictions_spark_df.show(10)  # Show a sample of the predictions
        else:
            print("No predictions generated (no valid groups).")

        return predictions_spark_df


    def fetch_data(self, start_date=None, end_date=None):
        """
        Orchestrate the entire process: read from Kafka, connect to Postgres,
        enrich, run transformations, and forecast with Prophet.
        """
        # Read from Kafka
        parsed_stream = self.read_kafka_stream()

        # Connect to Postgres with retry
        customer_data, product_data, retailer_data = self.connect_to_postgresql_with_retry()

        # Enrich the data
        enriched_stream = self.enrich_data(parsed_stream, customer_data, product_data, retailer_data)

        # Write to memory + wait for job to finish
        final_enriched_df = self.wait_for_data_and_collect(enriched_stream, start_date, end_date)

        # Show some records and counts
        final_enriched_df.show(10, truncate=False)
        record_count = final_enriched_df.count()
        print(f"\nNumber of enriched records: {record_count}\n")

        # Additional transformations
        final_df = self.preprocessing(final_enriched_df)

        return final_df

    def write_to_postgresql(self, dataframe, table_name='forecast', mode="overwrite"):
        """
        Write a Spark DataFrame to a PostgreSQL table.

        Parameters:
        - dataframe: Spark DataFrame to write
        - table_name: Name of the PostgreSQL table
        - mode: Write mode (default: 'overwrite'). Other options: 'append', 'ignore', 'error'.
        """
        try:
            logger.info(f"Writing DataFrame to PostgreSQL table '{table_name}' in '{mode}' mode...")
            dataframe.write.jdbc(
                url=self.jdbc_url,
                table=table_name,
                mode=mode,
                properties=self.jdbc_properties,
            )
            logger.info(f"Successfully wrote DataFrame to PostgreSQL table '{table_name}'.")
        except Exception as e:
            logger.error(f"Error writing DataFrame to PostgreSQL: {e}")
            raise


def main():
    """
    Entry point for running the entire job.
    """
    product_id = os.getenv("PRODUCT_ID")
    retailer_id = os.getenv("RETAILER_ID")

    if product_id:
        product_id = [int(pid) for pid in product_id.split(",")]  # Convert to list of integers

    if retailer_id:
        retailer_id = [int(rid) for rid in retailer_id.split(",")]  # Convert to list of integers

    start_date = os.getenv("START_DATE")
    end_date = os.getenv("END_DATE")
    forecast_duration = int(os.getenv("FORECAST_DURATION", 30))
    pipeline = SparkDataProcessAndForecast(retries=5, delay=5)

    parsed_stream = pipeline.read_kafka_stream()

    # Connect to Postgres with retry
    customer_data, product_data, retailer_data = pipeline.connect_to_postgresql_with_retry()

    enriched_stream = pipeline.enrich_data(parsed_stream, customer_data, product_data, retailer_data)

    # Write to memory + wait for job to finish
    final_enriched_df = pipeline.wait_for_data_and_collect(enriched_stream, start_date,
                                                           end_date, product_id, retailer_id)

    # Additional transformations
    final_df = pipeline.preprocessing(final_enriched_df)

    result = pipeline.prophet_forecast(final_df, forecast_duration)
    # Round the yhat result to integer and rename it to item_quantity
    result = result.withColumn("yhat", F.when(F.col("yhat") < 0, 0).otherwise(F.round(col("yhat")).cast(IntegerType())))
    result = result.withColumnRenamed("yhat", "item_quantity")
    if result.count() > 0:
        result[['ds', 'product_id', 'retailer_id', 'item_quantity']].write.jdbc(
            url=pipeline.jdbc_url,
            table='forecast',
            mode='overwrite',
            properties=pipeline.jdbc_properties,
        )
    else:
        print("No data to write to the database.")


if __name__ == "__main__":
    main()
