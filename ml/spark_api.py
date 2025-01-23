import logging
from spark import main

from flask import Flask, request, jsonify

# Configure logging
logging.basicConfig(level=logging.DEBUG)
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
        logger.info(f"Received forecast request: {data}")
        product_ids = data.get("product_ids")
        retailer_ids = data.get("retailer_ids")
        start_date = data.get("start_date")
        end_date = data.get("end_date")
        forecast_duration = data.get("forecast_duration", 7)

        if not product_ids or not retailer_ids or not start_date or not end_date:
            return jsonify({"error": "Missing required parameters"}), 400


        forecast_results = main(product_ids, retailer_ids, start_date, end_date, forecast_duration)

        if forecast_results:
            return jsonify({
                "message": "Forecast job completed successfully.",
                "forecast_results": forecast_results
            }), 200
        else:
            logger.warning("No forecast results generated.")
            return jsonify({"error": "No forecast results generated."}), 418

    except Exception as e:
        logger.error(f"Error in /start-forecast: {e}")
        return jsonify({"error": str(e)}), 500


if __name__ == "__main__":
    # Run the Flask app
    app.run(host="0.0.0.0", port=9003)
