import streamlit as st
import pandas as pd
import altair as alt
from db_util import load_static_data
from utils import process_orders

# Page configuration
st.set_page_config(page_title="Gross Sales Analysis", page_icon="📊", layout="wide")

st.markdown("# Gross Sales Analysis")
st.sidebar.header("Filter Options")

# Load static data
retailers, products = load_static_data()

# Load orders data from session state
if "orders_df" not in st.session_state:
    st.error("Orders data not found. Please go to the dashboard and reload data.")
    st.stop()

orders_df = st.session_state["orders_df"]
orders_df = process_orders(orders_df, retailers, products)

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
st.sidebar.header("Date Range")
start_date = st.sidebar.date_input("Start Date", value=filtered_orders["order_date"].min())
end_date = st.sidebar.date_input("End Date", value=filtered_orders["order_date"].max())

# Validate the date range
if start_date > end_date:
    st.error("Start date cannot be after end date.")
    st.stop()


# Filter orders based on the selected date range
filtered_orders = filtered_orders[(filtered_orders["order_date"] >= pd.to_datetime(start_date)) &
                                  (filtered_orders["order_date"] <= pd.to_datetime(end_date))]



# --- Matrix Plot: Total Items Sold by Retailer and Product ---
if not filtered_orders.empty:
    matrix_data = (
        filtered_orders.groupby(["retailer_name", "product_name"])["item_quantity"]
        .sum()
        .reset_index()
    )

    if not matrix_data.empty:
        matrix_chart = (
            alt.Chart(matrix_data)
            .mark_rect()
            .encode(
                x=alt.X("retailer_name:N", title="Retailer"),
                y=alt.Y("product_name:N", title="Product"),
                color=alt.Color("item_quantity:Q", title="Total Items Sold"),
                tooltip=["retailer_name:N", "product_name:N", "item_quantity:Q"],
            )
            .properties(
                title="Total Items Sold by Retailer and Product",
                width=800,
                height=400,
            )
        )

        st.altair_chart(matrix_chart, use_container_width=True)
    else:
        st.warning("No data available to generate the matrix plot.")
else:
    st.warning("No data available for the selected filters.")


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
