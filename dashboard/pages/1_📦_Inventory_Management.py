import streamlit as st
import altair as alt


st.set_page_config(page_title="Inventory Management", page_icon="📦", layout="wide")

st.markdown("# Inventory Management Dashboard")
st.sidebar.header("Filter Options")

# Load inventory data
if "inventory" not in st.session_state:
    st.error("Inventory data not found. Please go to the Dashboard and reload data.")
    st.stop()

inventory = st.session_state["inventory"]
products = st.session_state["products"]
retailers = st.session_state["retailers"]

inventory_df = inventory.merge(products, on="product_id", how="left")
inventory_df = inventory_df.merge(retailers, on="retailer_id", how="left")
# Sidebar filters for multi-select
selected_retailers = st.sidebar.multiselect("Select Retailers", inventory_df["retailer_name"].unique())
selected_products = st.sidebar.multiselect("Select Products", inventory_df["product_name"].unique())

# Apply filters
filtered_inventory = inventory_df
if selected_retailers:
    filtered_inventory = filtered_inventory[filtered_inventory["retailer_name"].isin(selected_retailers)]
if selected_products:
    filtered_inventory = filtered_inventory[filtered_inventory["product_name"].isin(selected_products)]

# --- KPI Metrics ---
st.subheader("Key Performance Indicators")

# Below reorder level
below_reorder = filtered_inventory[filtered_inventory["quantity_on_hand"] < filtered_inventory["reorder_level"]]
total_products = len(filtered_inventory)
below_reorder_percentage = (len(below_reorder) / total_products) * 100 if total_products > 0 else 0

# Layout the KPIs in a single row
kpi1, kpi2, kpi3, kpi4 = st.columns(4)

# Below reorder level
with kpi1:
    below_reorder = filtered_inventory[filtered_inventory["quantity_on_hand"] < filtered_inventory["reorder_level"]]
    total_products = len(filtered_inventory)
    below_reorder_percentage = (len(below_reorder) / total_products) * 100 if total_products > 0 else 0
    st.metric("Items Below Reorder Level", f"{below_reorder_percentage:.2f}%")

# Overstocked products
with kpi2:
    overstocked = filtered_inventory[
        filtered_inventory["quantity_on_hand"] > 1.5 * filtered_inventory["reorder_level"]
    ]
    st.metric("Overstocked Products", len(overstocked))

with kpi3:
    understocked = filtered_inventory[
        filtered_inventory["quantity_on_hand"] < 0.3 * filtered_inventory["reorder_level"]
    ]
    st.metric("Understocked Products", len(understocked))

# Total Inventory Value
with kpi4:
    
    if "product_price" in filtered_inventory.columns:
        total_inventory_value = (filtered_inventory["quantity_on_hand"] * filtered_inventory["product_price"]).sum()
        st.metric("Total Inventory Value", f"${total_inventory_value/10**6:,.2f}M")
    else:
        st.metric("Total Inventory Value", "N/A")
st.write("\n")
st.write("\n")
# --- Visualizations ---
st.subheader("Stock Levels by Product")

if not filtered_inventory.empty:
    filtered_inventory_a = filtered_inventory.drop_duplicates(subset=["product_id"], keep="first").copy()
    
    # Add a custom column for the label
    filtered_inventory_a.loc[:,"stock_status"] = filtered_inventory_a.apply(
        lambda row: "Low Stock" if row["quantity_on_hand"] < row["reorder_level"] else "High Stock",
        axis=1
    )
    
    stock_chart = (
        alt.Chart(filtered_inventory_a)
        .mark_bar()
        .encode(
            x=alt.X("product_id:N", title="Product ID"),
            y=alt.Y("quantity_on_hand:Q", title="Quantity on Hand"),
            color=alt.condition(
                alt.datum.quantity_on_hand < alt.datum.reorder_level,
                alt.value("red"),
                alt.value("green")
            ),
            tooltip=["product_name","product_id", "quantity_on_hand", "reorder_level", "stock_status"],
            
        )
        
    )
    
    st.altair_chart(stock_chart, use_container_width=True)
else:
    st.warning("No data available to display stock levels.")

# 3. Critical Products Table
if not below_reorder.empty:
    st.subheader("Critical Products Below Reorder Level")
    st.dataframe(below_reorder[["product_name","product_id", "retailer_name","retailer_id", "quantity_on_hand", "reorder_level"]], width=1200)
else:
    st.info("No products are currently below reorder level.")

