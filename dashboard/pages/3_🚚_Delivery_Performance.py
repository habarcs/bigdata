import streamlit as st
import pandas as pd
import altair as alt
import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'utils')))
from db_util import load_static_data

# Page configuration
st.set_page_config(page_title="Delivery Performance Dashboard", page_icon="🚚", layout="wide")

st.markdown("# Delivery Performance Dashboard")
st.sidebar.header("Filter Options")

# Load delivery data
if "orders_df" not in st.session_state:
    st.error("Delivery data not found. Please go to the dashboard and reload data.")
    st.stop()
orders_df = st.session_state["orders_df"]

# Sidebar filters
selected_regions = st.sidebar.multiselect("Select States", orders_df["retailer_state"].unique())
selected_status = st.sidebar.multiselect("Select Delivery Status", orders_df["delivery_status"].unique())

# Apply filters
filtered_deliveries = orders_df
if selected_regions:
    filtered_deliveries = filtered_deliveries[filtered_deliveries["retailer_state"].isin(selected_regions)]
if selected_status:
    filtered_deliveries = filtered_deliveries[filtered_deliveries["delivery_status"].isin(selected_status)]

# --- KPI Metrics ---
st.subheader("Key Performance Indicators")

# KPIs: On-time deliveries, delayed deliveries, total deliveries
on_time_deliveries = filtered_deliveries[filtered_deliveries["delivery_status"] == "COMPLETE"]
delayed_deliveries = filtered_deliveries[filtered_deliveries["delivery_status"] == "CANCELED"]
total_deliveries = len(filtered_deliveries)

on_time_rate = (len(on_time_deliveries) / total_deliveries) * 100 if total_deliveries > 0 else 0

kpi1, kpi2, kpi3 = st.columns(3)
with kpi1:
    st.metric("On-Time Delivery Rate", f"{on_time_rate:.2f}%", delta=f"{on_time_rate - 90:.2f}% Target")
with kpi2:
    st.metric("Total Deliveries", total_deliveries)
with kpi3:
    st.metric("Delayed Deliveries", len(delayed_deliveries))

st.write("\n")
st.write("\n")

# --- Visualizations ---
st.subheader("Delivery Performance Trends")

if not filtered_deliveries.empty:
    # Delivery trend over time
    trend_chart = (
        alt.Chart(filtered_deliveries)
        .mark_line()
        .encode(
            x=alt.X("order_date:T", title="Order Date"),
            y=alt.Y("count():Q", title="Number of Deliveries"),
            color=alt.Color(
                            "delivery_status", 
                            legend=alt.Legend(title="Delivery Status"), 
                        ),
            tooltip=["order_date:T", "delivery_status", "count()"]
        )
    )
    st.altair_chart(trend_chart, use_container_width=True)
else:
    st.warning("No data available to display delivery trends.")

# --- Regional Performance ---
st.subheader("Regional Delivery Performance")

if not filtered_deliveries.empty:
    regional_chart = (
        alt.Chart(filtered_deliveries)
        .mark_bar()
        .encode(
            x=alt.X("retailer_state:N", title="Region"),
            y=alt.Y("count():Q", title="Number of Deliveries"),
            color=alt.Color(
                            "delivery_status", 
                            legend=alt.Legend(title="Delivery Status"), 
                        ),
            tooltip=["retailer_state", "delivery_status", "count()"]
        )
    )
    st.altair_chart(regional_chart, use_container_width=True)
else:
    st.warning("No data available for regional performance.")

# --- Critical Delays Table ---
st.subheader("Cancelled Deliveries Details")

if not delayed_deliveries.empty:
    st.dataframe(delayed_deliveries[["retailer_id","product_id", "retailer_state", "delivery_status"]], width=1200)
else:
    st.info("No delayed deliveries to display.")
