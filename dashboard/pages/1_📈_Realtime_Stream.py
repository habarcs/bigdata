import time
import numpy as np
import pandas as pd
import plotly.express as px
import streamlit as st
from kafka import KafkaConsumer
import json
import sys 
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'utils')))

from db_util import get_engine
from kafka_consumer import kafka_realtime_consumer
# Streamlit configuration
st.set_page_config(
    page_title="Real-Time Supply Chain Dashboard",
    page_icon="📦",
    layout="wide",
)


consumer = kafka_realtime_consumer()

# Dashboard title
st.title("Real-Time Supply Chain Dashboard")

# Single-element container for live updates
placeholder = st.empty()

# Placeholder DataFrame for accumulating data
columns = [
    "transaction_type", "real_shipping_days", "scheduled_shipping_days",
    "delivery_status", "late_risk", "order_date", "order_id", "product_id",
    "item_quantity", "status", "shipping_data", "shipping_mode",
    "customer_id", "retailer_id"
]
data_df = pd.DataFrame(columns=columns)

# PostgreSQL connection setup
conn = get_engine()

# Simulated live updates
for message in consumer:
    # Fetch streaming data
    record = message.value
    data_df = pd.concat([data_df, pd.DataFrame([record])], ignore_index=True)


    # Add calculated fields
    data_df["gross_sell_value"] = (
        data_df["item_quantity"] * data_df["real_shipping_days"] * np.random.uniform(10, 100)
    )
    data_df["late_penalty"] = data_df["late_risk"] * np.random.uniform(5, 20)

    # Ensure order_date is in datetime format
    data_df["order_date"] = pd.to_datetime(data_df["order_date"])

    # Calculate KPIs
    avg_shipping_days = data_df["real_shipping_days"].mean()
    total_gross_sell = data_df["gross_sell_value"].sum()
    late_penalty_sum = data_df["late_penalty"].sum()

    with placeholder.container():
        # KPI columns
        kpi1, kpi2, kpi3 = st.columns(3)

        kpi1.metric(
            label="Avg Shipping Days 📦",
            value=round(avg_shipping_days, 2),
            delta=round(avg_shipping_days - data_df["scheduled_shipping_days"].mean(), 2),
        )

        kpi2.metric(
            label="Total Gross Sell 💰",
            value=f"${round(total_gross_sell, 2)}",
            delta=round(total_gross_sell / 100),
        )

        kpi3.metric(
            label="Total Late Penalty ⚠️",
            value=f"${round(late_penalty_sum, 2)}",
            delta=round(late_penalty_sum / len(data_df)),
        )

        # Charts
        fig_col1, fig_col2, fig_col3 = st.columns(3)

        with fig_col1:
            fig = px.histogram(
                data_df,
                x="delivery_status",
                title="Delivery Status Distribution",
                color_discrete_sequence=["#636EFA"],
            )
            st.write(fig)

        with fig_col2:
            fig2 = px.box(
                data_df,
                y="gross_sell_value",
                title="Gross Sell Value by Orders",
                color_discrete_sequence=["#EF553B"],
            )
            st.write(fig2)

        with fig_col3:
            data_df["hour"] = data_df["order_date"].dt.hour
            hourly_sales = (
                data_df.groupby([data_df["order_date"].dt.date, "hour"])["gross_sell_value"]
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

        # Detailed Data View
        st.markdown("### Detailed Data View")
        st.dataframe(data_df.tail(10))

        # Simulate real-time updates
        time.sleep(1)
