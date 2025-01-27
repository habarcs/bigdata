import plotly.express as px
import streamlit as st
import pandas as pd
import altair as alt
import numpy as np


# Page configuration
st.set_page_config(page_title="Sales Performance Dashboard", page_icon="📊", layout="wide")

st.markdown("# Sales Performance Dashboard")
st.sidebar.header("Filter Options")


# Load orders data from session state
if "orders_df" not in st.session_state:
    st.error("Orders data not found. Please go to the dashboard and reload data.")
    st.stop()

orders_df = st.session_state["orders_df"]

# Sidebar filters for multi-select
selected_retailers = st.sidebar.multiselect("Select Retailers", orders_df["retailer_name"].unique())
selected_products = st.sidebar.multiselect("Select Products", orders_df["product_name"].unique())

# Apply filters
filtered_orders = orders_df.copy(deep=True)
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
total_sales = filtered_orders["gross_sales"].sum()
total_units = filtered_orders["item_quantity"].sum()
mean_order_value = total_sales/filtered_orders.shape[0]
total_products = len(filtered_orders["product_name"].unique())
# KPI section
with st.container():
    kpi1, kpi2, kpi3, kpi4 = st.columns(4)

    kpi1.metric(
        label="Total Sales Revenue 💰",
        value=f"${round(total_sales/10**3, 2)}K",
    )

    kpi2.metric(
        label="Total Units Sold 📦",
        value=f"{round(total_units, 2)}",
    )
    kpi3.metric(
        label="Total Products Sold 📦",
        value=f"{round(total_products, 2)}",
    )
    
    kpi4.metric(
        label="Mean Order Price 💰",
        value=f"${round(mean_order_value, 2)}",
    )







if "late_penalty" not in orders_df.columns:
    orders_df["late_penalty"] = orders_df.get("late_risk", 0) * np.random.uniform(5, 20, len(orders_df))


if "category" in filtered_orders:
    category_sales = filtered_orders.groupby("category")["gross_sales"].sum().reset_index().sort_values("gross_sales", ascending=False)
    fig1 = px.bar(category_sales, x="category", y="gross_sales", title="Gross Sales by Category", labels={"category": "Category", "gross_sales": "Gross Sales"})
    st.write(fig1)
else:
    st.write("Category data is not available.")
    

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
                width=900,
                height=450,
            )
        )

        st.altair_chart(matrix_chart, use_container_width=True)
    else:
        st.warning("No data available to generate the matrix plot.")
else:
    st.warning("No data available for the selected filters.")


if all(col in filtered_orders for col in ["product_name", "gross_sales", "category"]):
    fig8 = px.box(
        filtered_orders,
        x="product_name",
        y="gross_sales",
        color="category",
        title="Profitability by Product",
        labels={"product_name": "Product", "gross_sales": "Gross Sales"},
        height=700,
    )
    st.write(fig8)
else:
    st.write("Required data for product profitability is not available.")


if "retailer_name" in filtered_orders and "product_name" in filtered_orders:
    retailer_product_sales = filtered_orders.groupby(["retailer_name", "product_name"])["gross_sales"].sum().reset_index()

    retailer_product_sales["gross_sales_log"] = np.log(retailer_product_sales["gross_sales"])
    retailer_product_sales["gross_sales_log"] = retailer_product_sales["gross_sales_log"].replace([np.inf, -np.inf, np.nan], 0)

    fig2 = px.treemap(
        retailer_product_sales,
        path=["retailer_name", "product_name"],  # Hierarchy: Retailer -> Product
        values="gross_sales",  # Gross sales for size
        title="Retailer Performance with Products",
        color="gross_sales_log",  # Use log scale for color
        color_continuous_scale="Viridis",  # Color scale
        hover_data={"gross_sales": ":,.2f"},  # Format hover data
    )

    # Display treemap in Streamlit
    st.plotly_chart(fig2, use_container_width=True)
else:
    st.write("Retailer or product data is not available.")


