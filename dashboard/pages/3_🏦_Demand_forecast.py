import streamlit as st
import pandas as pd
import altair as alt
import requests  # For making HTTP requests to the Flask API

import sys 
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'utils')))


from db_util import load_static_data
from primary import get_forecast_results, process_orders
from datetime import datetime

# Page configuration
st.set_page_config(page_title="Historical Data and Demand Forecast", page_icon="📊", layout="wide")

st.markdown("# Historical Data Visualization")
st.sidebar.header("Filter Options")

# Load static data
retailers, products = load_static_data()

# Load orders data from session state
if "orders_df" not in st.session_state:
    st.error("Orders data not found. Please go to the dashboard and reload data.")
    st.stop()

orders_df = st.session_state["orders_df"]

# Sidebar filters for multi-select
selected_retailers = st.sidebar.multiselect("Select Retailers", orders_df["retailer_name"].unique())
selected_products = st.sidebar.multiselect("Select Products", orders_df["product_name"].unique())

# Apply filters
filtered_orders = orders_df
if selected_retailers:
    filtered_orders = filtered_orders[filtered_orders["retailer_name"].isin(selected_retailers)]
if selected_products:
    filtered_orders = filtered_orders[filtered_orders["product_name"].isin(selected_products)]

# Ensure order_date is a datetime type
filtered_orders["order_date"] = pd.to_datetime(filtered_orders["order_date"])

# Sidebar filters for date range
if filtered_orders.order_date.count() > 0:
    st.sidebar.header("Date Range")
    start_date = st.sidebar.date_input("Start Date", value=filtered_orders["order_date"].min())
    end_date = st.sidebar.date_input("End Date", value=filtered_orders["order_date"].max())
else:
    st.error("No orders were found for these retailers and products.")
    st.stop()
    
# Validate the date range
if start_date > end_date:
    st.error("Start date cannot be after end date.")
    st.stop()

# Filter orders based on the selected date range
filtered_orders = filtered_orders[(filtered_orders["order_date"] >= pd.to_datetime(start_date)) &
                                  (filtered_orders["order_date"] <= pd.to_datetime(end_date))]

# --- Add Forecast Duration Option ---
st.sidebar.header("Predict Demand")
forecast_duration = st.sidebar.selectbox(
    "Select Forecast Duration (days)", 
    [7, 14, 30, 60, 90], 
    index=2  # Default to 30 days
)
filtered_ids = filtered_orders[["product_id", "retailer_id"]].drop_duplicates()

# --- Line Plot: Gross Sales Over Time by Retailer ---
if not filtered_orders.empty:
    retailer_sales = (
        filtered_orders.groupby(["order_date", "retailer_name"])["item_quantity"]
        .sum()
        .reset_index()
    )

    if not retailer_sales.empty:
        retailer_chart = (
            alt.Chart(retailer_sales)
            .mark_line()
            .encode(
                x="order_date:T",
                y="item_quantity:Q",
                color="retailer_name:N",
                tooltip=["order_date:T", "retailer_name:N", "item_quantity:Q"],
            )
            .properties(title="Total Product Sold Over Time by Retailer")
        )

        st.altair_chart(retailer_chart, use_container_width=True)
    else:
        st.warning("No data available for the selected retailers to generate the retailer sales chart.")


# --- Line Plot: Gross Sales Over Time by Product ---
if not filtered_orders.empty:
    product_sales = (
        filtered_orders.groupby(["order_date", "product_name"])["item_quantity"]
        .sum()
        .reset_index()
    )

    if not product_sales.empty:
        product_chart = (
            alt.Chart(product_sales)
            .mark_line()
            .encode(
                x="order_date:T",
                y="item_quantity:Q",
                color="product_name:N",
                tooltip=["order_date:T", "product_name:N", "item_quantity:Q"],
            )
            .properties(title="Total Product Sales Over Time")
        )

        st.altair_chart(product_chart, use_container_width=True)
    else:
        st.warning("No data available for the selected products to generate the product sales chart.")


if st.sidebar.button("Predict Demand"):
    st.markdown("# Demand Forecast")
    if selected_products and selected_retailers:
        # Prepare the request payload
        product_ids = filtered_ids["product_id"].unique().tolist()
        retailer_ids = filtered_ids["retailer_id"].unique().tolist()
        payload = {
            "product_ids": product_ids,
            "retailer_ids": retailer_ids,
            "start_date": start_date.strftime("%Y/%m/%d"),
            "end_date": end_date.strftime("%Y/%m/%d"),
            "forecast_duration": forecast_duration,
        }

        try:
            forecast_data = get_forecast_results(payload)

            if "forecast_results" in forecast_data and forecast_data["forecast_results"]:
                forecast_results = pd.DataFrame(forecast_data["forecast_results"])
                
                # Ensure consistent data types for merging
                forecast_results["product_id"] = forecast_results["product_id"].astype(str)
                forecast_results["retailer_id"] = forecast_results["retailer_id"].astype(str)

                products["product_id"] = products["product_id"].astype(str)
                retailers["retailer_id"] = retailers["retailer_id"].astype(str)

                # Map product and retailer IDs to names
                forecast_results = forecast_results.merge(products, on="product_id", how="left")
                forecast_results = forecast_results.merge(retailers, on="retailer_id", how="left")

                forecast_results["product_id"] = forecast_results["product_id"].astype(int)
                forecast_results["retailer_id"] = forecast_results["retailer_id"].astype(int)
                

                st.sidebar.success("Forecast received successfully!")

                # Separate forecasts by product
                st.write("### Demand Forecast by Product")
                product_forecasts = forecast_results.groupby(["ds", "product_name"])["item_quantity"].sum().reset_index()
                product_forecast_chart = (
                    alt.Chart(product_forecasts)
                    .mark_line()
                    .encode(
                        x="ds:T",
                        y="item_quantity:Q",
                        color="product_name:N",
                        tooltip=["ds:T", "product_name:N", "item_quantity:Q"],
                    )
                    .properties(title="Demand Forecast by Product", width=800, height=400)
                )
                st.altair_chart(product_forecast_chart, use_container_width=True)

                # Separate forecasts by retailer
                st.write("### Demand Forecast by Retailer")
                retailer_forecasts = forecast_results.groupby(["ds", "retailer_name"])["item_quantity"].sum().reset_index()
                retailer_forecast_chart = (
                    alt.Chart(retailer_forecasts)
                    .mark_line()
                    .encode(
                        x="ds:T",
                        y="item_quantity:Q",
                        color="retailer_name:N",
                        tooltip=["ds:T", "retailer_name:N", "item_quantity:Q"],
                    )
                    .properties(title="Demand Forecast by Retailer", width=800, height=400)
                )
                st.altair_chart(retailer_forecast_chart, use_container_width=True)

                # Separate forecasts by retailer
                # st.write("### Demand Forecast by Retailer")
                # retailer_forecasts = forecast_results.groupby(["ds", "retailer_name"])["item_quantity"].sum().reset_index()
                # retailer_forecast_chart = (
                #     alt.Chart(retailer_forecasts)
                #     .mark_line()
                #     .encode(
                #         x="ds:T",
                #         y="item_quantity:Q",
                #         color="retailer_name:N",
                #         tooltip=["ds:T", "retailer_name:N", "item_quantity:Q"],
                #     )
                #     .properties(title="Demand Forecast by Retailer", width=800, height=400)
                # )
                # st.altair_chart(retailer_forecast_chart, use_container_width=True)
            else:
                st.sidebar.warning("No forecast results returned from the API.")

        except RuntimeError as e:
            st.sidebar.error(str(e))
    else:
        st.sidebar.error("Please select at least one product and one retailer.")

# Add your previous historical data plots here, such as "Total Items Sold by Retailer and Product"
