import time
import warnings

from pyspark.sql.types import IntegerType
from pyspark.sql.functions import col, when

warnings.filterwarnings("ignore")
import logging

from pyspark.sql import SparkSession
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
            SparkSession.builder.appName("SparkConnector")
            .config("spark.jars", "/opt/bitnami/spark/jars/postgresql-42.7.4.jar") # used on the docker image
            # .config("spark.jars", "./postgresql-42.7.4.jar") # used if ran locally
            .getOrCreate()
        )

        # Store JDBC properties
        self.jdbc_url = "jdbc:postgresql://sql-database:5432/postgres"  # sql-database - service name from Docker Compose
        self.jdbc_properties = {
            "user": "postgres",
            "password": "supersecret",
            "driver": "org.postgresql.Driver",
        }

        # Retry logic configs
        self.retries = retries
        self.delay = delay

    @staticmethod
    def _list_to_sql_list(predicate: list):
        return "(" +  ", ".join([str(p) for p in predicate]) + ")"

    def connect_to_postgresql_with_retry(self, retailer_ids: list[int], product_ids: list[int],
                                         start_date: str, end_date: str):
        """
        Connect to Postgres with retry logic. Returns aggregated history, product, and retailer dataframes.
        """
        logger.info("Connecting to Postgres with retries...")

        for attempt in range(self.retries):
            try:
                aggregated_history = self.spark.read.jdbc(
                    self.jdbc_url, "daily_aggregates", properties=self.jdbc_properties,
                    predicates=[f"product_id in {self._list_to_sql_list(product_ids)} "
                                f"AND retailer_id in {self._list_to_sql_list(retailer_ids)} "
                                f"AND ds BETWEEN '{start_date}' AND '{end_date}'"]
                )
                product_data = self.spark.read.jdbc(
                    self.jdbc_url, "products", properties=self.jdbc_properties,
                    predicates=[f"product_id in {self._list_to_sql_list(product_ids)}"]
                )
                retailer_data = self.spark.read.jdbc(
                    self.jdbc_url, "retailers", properties=self.jdbc_properties,
                    predicates=[f"retailer_id in {self._list_to_sql_list(retailer_ids)}"]
                )
                logger.info("Connection to Postgres successful.")
                return aggregated_history, product_data, retailer_data
            except Exception as e:
                logger.error(f"Attempt {attempt + 1} failed: {e}")
                if attempt < self.retries - 1:
                    time.sleep(self.delay)
                else:
                    raise

    def join_data(self, aggregated_history, product_data, retailer_data):
        """
        Join the tables together
        """
        joined_data = \
            aggregated_history \
            .join(product_data, "product_id", "left") \
            .join(retailer_data, "retailer_id", "left")

        return joined_data

    def prophet_forecast(self, data, days=7):
        """
        Train Prophet models for each retailer_id/product_id combination
        and generate predictions for future dates only.
        """

        # Train Prophet models
        models = {}
        retailer_product_groups = data.groupby(["retailer_id", "product_id"]).count().collect()

        for row in retailer_product_groups:
            product_id = row['product_id']
            retailer_id = row['retailer_id']
            df = data.filter(
                (data.product_id == product_id) &
                (data.retailer_id == retailer_id)
            ) \
                .select(col("ds"), col("total_item_quantity")) \
                .groupby("ds") \
                .sum("total_item_quantity")\
                .withColumnRenamed("sum(total_item_quantity)", "y") \
                .sort("ds")

            if df.count() < 2:
                print(f"Skipping retailer_id: {retailer_id}, product_id: {product_id} (insufficient data).")
                continue

            # Initialize Prophet model
            model = Prophet(yearly_seasonality=False, weekly_seasonality=False, daily_seasonality=False)
            model.add_seasonality(name='monthly', period=30.5, fourier_order=5)
            model.fit(df.toPandas())

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

        # Combine all predictions into a single DataFrame
        if future_predictions:
            predictions_df = pd.concat(future_predictions, ignore_index=True)
            predictions_spark_df = self.spark.createDataFrame(predictions_df.astype(str))
            predictions_spark_df.show(10)  # Show a sample of the predictions
            return predictions_spark_df
        print("No predictions generated (no valid groups).")
        return None



def main(product_ids: list[int], retailer_ids: list[int], start_date: str, end_date: str, forecast_duration: int):
    """
    This function is able to run the spark prediction locally used for debug purposes
    :return:
    """

    # Initialize Spark pipeline
    pipeline = SparkDataProcessAndForecast()

    # Connect to Postgres with retry
    aggregated_history, product_data, retailer_data = pipeline.connect_to_postgresql_with_retry(
        retailer_ids, product_ids, start_date, end_date
    )

    data = pipeline.join_data(
        aggregated_history, product_data, retailer_data
    )

    # Perform forecast
    result = pipeline.prophet_forecast(data, forecast_duration)

    if result:
        # Format and return results
        formatted_result = result.withColumn(
            "yhat",
            when(col("yhat") < 0, 0).otherwise(col("yhat").cast(IntegerType()))
        ).withColumnRenamed("yhat", "item_quantity")

        # Collect and convert to JSON
        forecast_results = formatted_result.select(
            "ds", "product_id", "retailer_id", "item_quantity"
        ).toPandas().to_dict(orient="records")

        return forecast_results
    return None
