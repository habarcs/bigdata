from flask import Flask, request, jsonify
import logging
import os
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import col, when
from spark import SparkDataProcessAndForecast  # Import the class from spark.py

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Create Flask app
app = Flask(__name__)

@app.route('/start-forecast', methods=['POST'])
def start_forecast():
    """
    REST API endpoint to run the Spark job and return forecast results.
    """
    try:
        # Parse request data
        data = request.json
        product_ids = data.get("product_ids")
        retailer_ids = data.get("retailer_ids")
        start_date = data.get("start_date")
        end_date = data.get("end_date")
        forecast_duration = data.get("forecast_duration", 7)

        if not product_ids or not retailer_ids or not start_date or not end_date:
            return jsonify({"error": "Missing required parameters"}), 400

        logger.info(f"Received forecast request: {data}")

        # Initialize Spark pipeline
        pipeline = SparkDataProcessAndForecast()
        parsed_stream = pipeline.read_kafka_stream()

        # Connect to Postgres with retry
        customer_data, product_data, retailer_data = pipeline.connect_to_postgresql_with_retry()

        enriched_stream = pipeline.enrich_data(
            parsed_stream, customer_data, product_data, retailer_data
        )

        # Write to memory + wait for job to finish
        final_enriched_df = pipeline.wait_for_data_and_collect(
            enriched_stream, start_date, end_date, product_ids, retailer_ids
        )

        # Additional transformations
        final_df = pipeline.preprocessing(final_enriched_df)

        # Perform forecast
        result = pipeline.prophet_forecast(final_df, forecast_duration)

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

            return jsonify({
                "message": "Forecast job completed successfully.",
                "forecast_results": forecast_results
            }), 200
        else:
            logger.warning("No forecast results generated.")
            return jsonify({"message": "No forecast results generated."}), 204

    except Exception as e:
        logger.error(f"Error in /start-forecast: {e}")
        return jsonify({"error": str(e)}), 500


if __name__ == "__main__":
    # Run the Flask app
    app.run(host="0.0.0.0", port=4040)
