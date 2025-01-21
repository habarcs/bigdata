import streamlit as st
from utils.kafka_consumer import consume_kafka_data
from utils.db_util import load_static_data
from utils.primary import process_orders, find_new_products
import pandas as pd
import numpy as np
import time
import plotly.express as px

# Page configuration
st.set_page_config(page_title="Retailers and Products Streaming", page_icon="📊", layout="wide")

st.markdown("# Retailers and Products Streaming Dashboard")
st.sidebar.header("Filter Options")

# Load static data
retailers, products = load_static_data()

# State to control data loading
if "orders_df" not in st.session_state:
    st.session_state["orders_df"] = consume_kafka_data()
    st.session_state['retailers'] = retailers
    st.session_state['products'] = products

# Reload data button with progress bar
if st.button("Reload Data from Kafka"):
    with st.spinner("Reloading data..."):
        progress = st.progress(0)
        for i in range(100):
            time.sleep(0.01)  # Simulate progress
            progress.progress(i + 1)
        new_orders = consume_kafka_data()
        progress.progress(100)

        new_product_ids = find_new_products(new_orders, st.session_state["orders_df"])
        st.session_state["new_product_ids"] = new_product_ids
        st.session_state["orders_df"] = new_orders

# Get data from session state
orders_df = st.session_state["orders_df"]
orders_df = process_orders(orders_df, retailers, products)

# Add calculated fields for KPIs
orders_df["gross_sell_value"] = (
    orders_df["item_quantity"] * np.random.uniform(10, 100, len(orders_df))
)
orders_df["late_penalty"] = orders_df.get("late_risk", 0) * np.random.uniform(1, 5, len(orders_df))

# Sidebar filters for multi-select
selected_retailers = st.sidebar.multiselect("Select Retailers", orders_df["retailer_name"].unique())
selected_products = st.sidebar.multiselect("Select Products", orders_df["product_name"].unique())

# Apply filters based on selected retailer-product pair
filtered_orders = orders_df
if selected_retailers:
    filtered_orders = filtered_orders[filtered_orders["retailer_name"].isin(selected_retailers)]
if selected_products:
    filtered_orders = filtered_orders[filtered_orders["product_name"].isin(selected_products)]

# Calculate KPIs for filtered data
avg_shipping_days = filtered_orders["real_shipping_days"].mean()
total_gross_sell = filtered_orders["gross_sell_value"].sum()
late_penalty_sum = filtered_orders["late_penalty"].sum()

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
        value=f"${round(total_gross_sell, 2)}",
    )

    kpi3.metric(
        label="Total Record 📋",
        value=f"{round(total_count, 2)}",
    )
    
    kpi4.metric(
        label="Total Late Penalty ⚠️",
        value=f"${round(late_penalty_sum, 2)}",
    )

# Visualizations section
with st.container():
    fig_col1, fig_col2, fig_col3 = st.columns(3)

    with fig_col1:
        if "delivery_status" in filtered_orders:
            fig = px.histogram(
                filtered_orders,
                x="delivery_status",
                title="Delivery Status Distribution",
                color_discrete_sequence=["#636EFA"],
            )
            st.write(fig)

    with fig_col2:
        fig2 = px.box(
            filtered_orders,
            y="gross_sell_value",
            title="Gross Sell Value by Orders",
            color_discrete_sequence=["#EF553B"],
        )
        st.write(fig2)

    with fig_col3:
        if "order_date" in filtered_orders:
            filtered_orders["order_date"] = pd.to_datetime(filtered_orders["order_date"])
            filtered_orders["hour"] = filtered_orders["order_date"].dt.hour
            hourly_sales = (
                filtered_orders.groupby([filtered_orders["order_date"].dt.date, "hour"])["gross_sell_value"]
                .sum()
                .reset_index()
            )
            hourly_sales.columns = ["order_date", "hour", "gross_sales"]
            hourly_sales["datetime"] = pd.to_datetime(hourly_sales["order_date"]) + pd.to_timedelta(hourly_sales["hour"], unit="h")
            fig3 = px.line(
                hourly_sales,
                x="datetime",
                y="gross_sales",
                title="Gross Sales Over Hours",
                markers=True,
                labels={"gross_sales": "Gross Sales", "datetime": "Time"},
            )
            st.write(fig3)


with st.container():
    fig_col1, fig_col2  = st.columns(2)

    with fig_col1:
        df_retailers = filtered_orders.groupby("retailer_name")["item_quantity"].sum().reset_index()
        fig = px.bar(
            df_retailers,
            x="retailer_name",
            y="item_quantity",
            title="Count of Items Sold by Retailer",
            color_discrete_sequence=["#636EFA"],
        )
        st.write(fig)
        
    with fig_col2:
        df_productss = filtered_orders.groupby("product_name")["item_quantity"].sum().reset_index()
        fig = px.bar(
            df_productss,
            x="product_name",
            y="item_quantity",
            title="Count of Items Sold by Product",
            color_discrete_sequence=["#636EFA"],
        )
        st.write(fig)
