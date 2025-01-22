import streamlit as st
from utils.kafka_consumer import consume_kafka_data
from utils.db_util import load_static_data
from utils.primary import process_orders, find_new_products, get_postgres_data
import pandas as pd
import numpy as np
import time
import plotly.express as px
from datetime import datetime, timedelta

# Page configuration
st.set_page_config(
    page_title="Supply Chain Management System", page_icon="📊", layout="wide"
)

st.markdown("# Welcome to Supply Chain Optimization Dashboard")
st.markdown("##### Here You can monitor the performance of your supply chain")

st.sidebar.header("Filter Options")

# Fetch data from the PostgreSQL database
query = """
SELECT *
FROM daily_aggregates
WHERE ds::DATE BETWEEN (SELECT MAX(ds::DATE) FROM daily_aggregates) - INTERVAL '30 days'
AND (SELECT MAX(ds::DATE) FROM daily_aggregates);
"""

orders = get_postgres_data(query)
retailers, products = load_static_data()
inventory = get_postgres_data("SELECT * FROM inventory")
st.session_state["retailers"] = retailers
st.session_state["products"] = products
st.session_state["inventory"] = inventory

orders_df = process_orders(orders, retailers, products, inventory)
orders_df["order_date"] = pd.to_datetime(orders_df["order_date"])

start_date = st.sidebar.date_input(
    "Start Date",
    value=(
        orders_df["order_date"].min().date()
        if not orders_df.empty
        else datetime(2015, 1, 1).date()
    ),
)
end_date = st.sidebar.date_input(
    "End Date",
    value=(
        orders_df["order_date"].max().date()
        if not orders_df.empty
        else datetime(2025, 1, 30).date()
    ),
)

# Initialize session state for dates if not already set
if "start_date" not in st.session_state:
    st.session_state["start_date"] = pd.to_datetime(start_date)
if "end_date" not in st.session_state:
    st.session_state["end_date"] = pd.to_datetime(end_date)

def adjust_date_range(date_range):
    # Use session state for end_date, fallback if not present
    end_date = st.session_state.get("end_date", pd.Timestamp("2025-01-30"))
    # Compute start_date based on the selected range
    if date_range == "Previous 15 Days":
        start_date = end_date - timedelta(days=15)
    elif date_range == "Previous Week":
        start_date = end_date - timedelta(weeks=1)
    elif date_range == "Previous Month":
        start_date = end_date - timedelta(days=30)
    elif date_range == "Previous Year":
        start_date = end_date - timedelta(days=365)
    else:
        # Default to Previous Month
        start_date = end_date - timedelta(days=30)
    return max(start_date, pd.Timestamp("2015-01-01")), end_date

# Date range selection with proper session state handling
date_range = st.sidebar.selectbox(
    "Select Date Range",
    ["Previous Week", "Previous 15 Days", "Previous Month", "Previous Year"],
)
start_date, end_date = adjust_date_range(date_range)

# Update session state for dates
st.session_state["start_date"] = start_date
st.session_state["end_date"] = end_date

# State to control data loading
if "orders_df" not in st.session_state:
    st.session_state["orders_df"] = orders_df

if st.sidebar.button("Apply Data Range and Reload Data"):
    # Update session state for selected dates
    st.session_state["start_date"] = start_date
    st.session_state["end_date"] = end_date
    # Fetch filtered data
    query = f"""
    SELECT *
    FROM daily_aggregates
    WHERE ds::DATE BETWEEN '{st.session_state["start_date"].strftime('%Y-%m-%d')}'
    AND '{st.session_state["end_date"].strftime('%Y-%m-%d')}';
    """
    with st.spinner("Reloading data..."):
        progress = st.progress(0)
        for i in range(0, 101, 10):
            time.sleep(0.05)
            progress.progress(i)
        new_orders = get_postgres_data(query)
        progress.progress(100)

        st.session_state["orders"] = new_orders

# Get data from session state
if "orders" in st.session_state:
    orders_df = st.session_state["orders"]
    orders_df = process_orders(orders_df, retailers, products, inventory)
    st.session_state['orders_df'] = orders_df

orders_df["late_penalty"] = orders_df.get("late_risk", 0) * np.random.uniform(
    1, 5, len(orders_df)
)

filtered_orders = orders_df







# Calculate KPIs for filtered data
avg_shipping_days = filtered_orders["real_shipping_days"].mean()
total_gross_sell = filtered_orders["gross_sales"].sum()
reorder_avg = filtered_orders["reorder_level"].mean()

# Total count after filtering
total_count = len(filtered_orders)

# KPI section
with st.container():
    kpi1, kpi2, kpi3, kpi4 = st.columns(4)

    kpi1.metric(
        label="Avg Shipping Days 📦",
        value=round(avg_shipping_days, 2),
    )

    kpi2.metric(
        label="Total Gross Sell 💰",
        value=f"${round(total_gross_sell/10**6, 2)}M",
    )
    
    kpi3.metric(
        label="Mean Reorder Value 🔄",
        value=f"{round(reorder_avg, 2)}",
    )

    kpi4.metric(
        label="Total Orders 📋",
        value=f"{round(total_count, 2)}",
    )

# Visualizations section
with st.container():
    fig_col3, fig_col2, fig_col1 = st.columns(3)

    # Plot 1: Order Status Distribution
    with fig_col1:
        delivery_status_count = filtered_orders["delivery_status"].value_counts().sort_values(ascending=False)
        
        fig = px.bar(
            delivery_status_count.index,
            x="delivery_status",
            y= delivery_status_count.values,
            title="Order Status Distribution",
            color=delivery_status_count.values,
            height=400,
            width=400,
        )
        fig.update_layout(
            xaxis=dict(tickangle=45),  # Rotate x-axis labels
            title=dict(font=dict(size=16)),
        )
        st.write(fig)

    # Plot 2: Top 10 Retailers and Quantity Sold
    with fig_col2:
        df_retailers = (
            filtered_orders.groupby("retailer_name")["item_quantity"]
            .sum()
            .reset_index()
        ).nlargest(10, "item_quantity")
        
        fig = px.bar(
            df_retailers,
            x="retailer_name",
            y="item_quantity",
            title="Top 10 Retailers and Quantity Sold",
            color="item_quantity",
            height=450,
            width=600,  # Ensure consistent width
        )
        fig.update_layout(
            xaxis=dict(tickangle=45),  # Rotate x-axis labels
            title=dict(font=dict(size=16)),
        )
        st.write(fig)

    # Plot 3: Top 10 Sold Products
    with fig_col3:
        df_products = (
            filtered_orders.groupby("product_name")["item_quantity"].sum().reset_index()
        ).nlargest(10, "item_quantity")
        
        fig = px.bar(
            df_products,
            x="product_name",
            y="item_quantity",
            title="Top 10 Sold Products",
            color="item_quantity",
            height=500,
            width=600,  # Ensure consistent width
            labels={"product_name": "Product Name", "item_quantity": "Quantity Sold"},
        )
        fig.update_layout(
            xaxis=dict(tickangle=45),  # Rotate x-axis labels
            title=dict(font=dict(size=16)),
        )
        st.write(fig)

