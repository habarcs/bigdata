import streamlit as st
import plotly.express as px
import pandas as pd
import numpy as np

# Assuming `orders_df` is available in the session state
if "orders_df" not in st.session_state:
    st.error("No orders data available. Please load data on the main page.")
    st.stop()

orders_df = st.session_state["orders_df"]

# Add calculated columns if not already present
if "gross_sell_value" not in orders_df:
    orders_df["gross_sell_value"] = (
        orders_df["item_quantity"] * np.random.uniform(10, 100, len(orders_df))
    )

if "late_penalty" not in orders_df:
    orders_df["late_penalty"] = orders_df.get("late_risk", 0) * np.random.uniform(5, 20, len(orders_df))

# Ensure 'order_date' is datetime
if "order_date" in orders_df:
    orders_df["order_date"] = pd.to_datetime(orders_df["order_date"])

# Page title
st.title("Advanced Data Analysis and Visualizations")

# 1. Product Category Analysis
st.subheader("1. Product Category Analysis")
if "category" in orders_df:
    category_sales = orders_df.groupby("category")["gross_sell_value"].sum().reset_index()
    fig1 = px.bar(category_sales, x="category", y="gross_sell_value", title="Gross Sales by Category", color="category")
    st.write(fig1)
else:
    st.write("Category data is not available.")

# 2. Retailer Performance
st.subheader("2. Retailer Performance")
if "retailer_name" in orders_df:
    retailer_sales = orders_df.groupby("retailer_name")["gross_sell_value"].sum().reset_index()
    fig2 = px.treemap(retailer_sales, path=["retailer_name"], values="gross_sell_value", title="Retailer Performance")
    st.write(fig2)
else:
    st.write("Retailer data is not available.")

# 3. Late Risk vs. Shipping Days
st.subheader("3. Late Risk vs. Shipping Days")
required_columns = ["real_shipping_days", "late_risk", "gross_sell_value"]
if all(col in orders_df for col in required_columns):
    filtered_df = orders_df.dropna(subset=required_columns)
    fig3 = px.scatter(
        filtered_df,
        x="real_shipping_days",
        y="late_risk",
        size="gross_sell_value",
        color="delivery_status",
        title="Late Risk vs. Shipping Days",
    )
    st.write(fig3)
else:
    st.write("Required data for Late Risk vs. Shipping Days analysis is not available.")

# 4. Hourly Order Trends
st.subheader("4. Hourly Order Trends")
if "order_date" in orders_df:
    orders_df["hour"] = orders_df["order_date"].dt.hour
    hourly_sales = (
        orders_df.groupby(["hour"])["gross_sell_value"].sum().reset_index()
    )
    fig4 = px.bar(hourly_sales, x="hour", y="gross_sell_value", title="Hourly Gross Sales", labels={"hour": "Hour of Day", "gross_sell_value": "Gross Sales"})
    st.write(fig4)
else:
    st.write("Order date data is not available for hourly trends.")

# 5. Cumulative Gross Sales
st.subheader("5. Cumulative Gross Sales")
if "order_date" in orders_df:
    daily_sales = orders_df.groupby(orders_df["order_date"].dt.date)["gross_sell_value"].sum().reset_index()
    daily_sales["cumulative_sales"] = daily_sales["gross_sell_value"].cumsum()
    fig5 = px.line(daily_sales, x="order_date", y="cumulative_sales", title="Cumulative Gross Sales Over Time")
    st.write(fig5)
else:
    st.write("Order date data is not available for cumulative sales.")

# 6. High-Risk Orders
st.subheader("6. High-Risk Orders")
if all(col in orders_df for col in ["gross_sell_value", "late_risk", "delivery_status"]):
    high_risk_orders = orders_df[orders_df["late_risk"] > 0]
    fig6 = px.scatter(
        high_risk_orders,
        x="gross_sell_value",
        y="late_risk",
        color="delivery_status",
        size="item_quantity",
        title="High-Risk Orders (Gross Sales vs. Late Risk)",
        labels={"gross_sell_value": "Gross Sales", "late_risk": "Late Risk"},
    )
    st.write(fig6)
else:
    st.write("Required data for high-risk orders is not available.")

# 7. Order Quantity Distribution
st.subheader("7. Order Quantity Distribution")
if "item_quantity" in orders_df:
    fig7 = px.histogram(
        orders_df,
        x="item_quantity",
        title="Order Quantity Distribution",
        nbins=20,
        labels={"item_quantity": "Order Quantity"},
    )
    st.write(fig7)
else:
    st.write("Order quantity data is not available.")

# 8. Profitability by Product
st.subheader("8. Profitability by Product")
if all(col in orders_df for col in ["product_name", "gross_sell_value", "category"]):
    fig8 = px.box(
        orders_df,
        x="product_name",
        y="gross_sell_value",
        color="category",
        title="Profitability by Product",
        labels={"product_name": "Product", "gross_sell_value": "Gross Sales"},
    )
    st.write(fig8)
else:
    st.write("Required data for product profitability is not available.")

# Inform users if no data is available
if orders_df.empty:
    st.write("No data available to display visualizations.")
