import pandas as pd
from kafka import KafkaProducer
import json
import requests


def process_orders(orders_df, retailers, products):
    """
    Joins orders with retailers and products and calculates gross sales.
    """
    # Join with retailers and products
    orders_df = orders_df.merge(retailers, on="retailer_id", how="left")
    orders_df = orders_df.merge(products, on="product_id", how="left")
    orders_df.sort_values(by="item_quantity", ascending=False, inplace=True, ignore_index=True)

    # Add gross sales column
    orders_df["gross_sales"] = orders_df["item_quantity"] * orders_df["product_price"]

    # Select relevant columns
    orders_df = orders_df[
        [
            "order_id",
            "order_date",
            "retailer_name",
            "retailer_country",
            "product_id",
            "retailer_id",
            "product_name",
            "category",
            "item_quantity",
            "product_price",
            "status",
            "shipping_mode",
            "gross_sales",
            "real_shipping_days",
            "delivery_status",
            "late_risk",
        ]
    ]
    return orders_df


def find_new_products(new_orders, old_orders):
    """
    Identifies new products from new_orders compared to old_orders.
    """
    if "product_id" in old_orders.columns:
        old_product_ids = set(old_orders["product_id"].unique())
    else:
        old_product_ids = set()

    new_product_ids = set(new_orders["product_id"].unique()) - old_product_ids
    return new_product_ids



# Function to publish messages to Kafka
def publish_to_kafka(topic, message):
    try:
        producer = KafkaProducer(
            bootstrap_servers="localhost:9092",
            value_serializer=lambda v: json.dumps(v).encode("utf-8")
        )
        producer.send(topic, message)
        producer.flush()
        producer.close()
    except Exception as e:
        raise RuntimeError(f"Failed to publish message to Kafka: {str(e)}")
    
    

def get_forecast_results(api_url, payload):
    """
    Fetch forecast results from the REST API.

    Parameters:
    - api_url (str): The endpoint of the Flask REST API.
    - payload (dict): The data to send in the POST request.

    Returns:
    - dict: The parsed JSON response containing forecast results or an error message.
    """
    try:
        response = requests.post(api_url, json=payload)
        response.raise_for_status()  # Raise an exception for HTTP errors
        return response.json()
    except requests.exceptions.RequestException as e:
        raise RuntimeError(f"Failed to fetch forecast results: {e}")
