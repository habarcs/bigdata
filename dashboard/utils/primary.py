import pandas as pd
import psycopg2
import requests
from dotenv import load_dotenv

load_dotenv()
import os

SPARK_SERVER = os.getenv("SPARK_SERVER")
SQL_ADDRESS = os.getenv("SQL_ADDRESS")


def process_orders(orders_df, retailers, products, inventory):
    """
    Joins orders with retailers and products and calculates gross sales.
    """
    # Join with retailers and products
    orders_df = orders_df.merge(retailers, on="retailer_id", how="left")
    orders_df = orders_df.merge(products, on="product_id", how="left")
    orders_df = orders_df.merge(inventory, on=["product_id","retailer_id"], how="left")
    orders_df.sort_values(
        by="total_item_quantity", ascending=False, inplace=True, ignore_index=True
    )
    
    # Add gross sales column
    orders_df["gross_sales"] = (
        orders_df["total_item_quantity"] * orders_df["product_price"]
    )
    orders_df.rename(
        columns={
            "ds": "order_date",
            "total_item_quantity": "item_quantity",
            "avg_real_shipping_days": "real_shipping_days",
            "avg_scheduled_shipping_days": "scheduled_shipping_days",
            "avg_late_risk": "late_risk",
            "order_status": "delivery_status",
        },
        inplace=True,
    )

    # Select relevant columns
    orders_df = orders_df[
        [
            "order_date",
            "retailer_name",
            "retailer_country",
            "retailer_state",
            "product_id",
            "retailer_id",
            "product_name",
            "category",
            "item_quantity",
            "product_price",
            "gross_sales",
            "real_shipping_days",
            "scheduled_shipping_days",
            "delivery_status",
            "late_risk",
            "quantity_on_hand",
            "reorder_level"
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


def get_forecast_results(payload):
    """
    Fetch forecast results from the REST API.

    Parameters:
    - api_url (str): The endpoint of the Flask REST API.
    - payload (dict): The data to send in the POST request.

    Returns:
    - dict: The parsed JSON response containing forecast results or an error message.
    """
    forecast_api_url = f"http://{SPARK_SERVER}/start-forecast"
    try:
        response = requests.post(forecast_api_url, json=payload)
        response.raise_for_status()  # Raise an exception for HTTP errors
        return response.json()
    except requests.exceptions.RequestException as e:
        raise RuntimeError(f"Failed to fetch forecast results: {e}")


def get_postgres_data(query):
    conn = psycopg2.connect(
        host=SQL_ADDRESS,
        port="5432",
        database="postgres",
        user="postgres",
        password="supersecret",
    )
    try:
        df = pd.read_sql(query, conn)
    finally:
        conn.close()
    return df
