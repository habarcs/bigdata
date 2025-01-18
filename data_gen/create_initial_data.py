from itertools import product

import faker
import numpy as np
import pandas as pd
import sqlalchemy


def create_products(df: pd.DataFrame, engine: sqlalchemy.engine.Engine):
    if data_already_created(engine, "products"):
        return
    products = df.drop_duplicates(subset="Product Card Id", keep="first")[[
        "Product Card Id",
        "Product Name",
        "Category Name",
        "Product Price"
    ]].rename(columns={
        "Product Card Id": "product_id",
        "Product Name": "product_name",
        "Category Name": "category",
        "Product Price": "product_price"
    })
    products.to_sql(name="products",
                    con=engine,
                    if_exists="append",
                    index=False)
    set_data_created(engine, "products")


def create_retailers(df: pd.DataFrame, engine: sqlalchemy.engine.Engine):
    if data_already_created(engine, "retailers"):
        return
    fake = faker.Faker()
    retailers = df.drop_duplicates(subset=["Customer City", "Customer State", "Customer Country"], keep="first")[[
        "Customer Country",
        "Customer State",
        "Customer City",
    ]].rename(columns={
        "Customer Country": "retailer_country",
        "Customer State": "retailer_state",
        "Customer City": "retailer_city"
    })
    retailers["retailer_name"] = [fake.unique.company() for _ in retailers.index]

    retailers.to_sql(name="retailers",
                     con=engine,
                     if_exists="append",
                     index=False)
    set_data_created(engine, "retailers")


def create_inventory(_, engine: sqlalchemy.engine.Engine):
    if data_already_created(engine, "inventory"):
        return
    with engine.connect() as conn:
        retailer_ids = conn.execute(sqlalchemy.text('SELECT retailer_id FROM retailers')).fetchall()
        product_ids = conn.execute(sqlalchemy.text('SELECT product_id FROM products')).fetchall()

    retailer_ids = [retailer_id[0] for retailer_id in retailer_ids]
    product_ids = [product_id[0] for product_id in product_ids]
    df = pd.DataFrame(product(retailer_ids, product_ids),
                      columns=["retailer_id", "product_id"])
    np.random.seed = 3154257842
    df["quantity_on_hand"] = np.random.randint(100, 10000, df.shape[0])
    df["reorder_level"] = np.random.randint(0, 10000, df.shape[0])

    df.to_sql(name="inventory",
              con=engine,
              if_exists="append",
              index=False)
    set_data_created(engine, "inventory")


def create_locations(df: pd.DataFrame, engine: sqlalchemy.engine.Engine):
    if data_already_created(engine, "locations"):
        return
    # map the Customer Region from Customer State (as it was not given)
    state_to_region = {
        'CA': 'West of USA', 'OR': 'West of USA', 'WA': 'West of USA', 'NV': 'West of USA',
        'AZ': 'West of USA', 'UT': 'West of USA', 'HI': 'West of USA', 'ID': 'West of USA',
        'MT': 'West of USA', 'CO': 'West of USA',
        'ND': 'US Center', 'SD': 'US Center', 'NE': 'US Center', 'KS': 'US Center',
        'OK': 'US Center', 'IA': 'US Center', 'MO': 'US Center', 'MN': 'US Center',
        'IL': 'US Center', 'WI': 'US Center', 'IN': 'US Center', 'MI': 'US Center',
        'NY': 'East of USA', 'MA': 'East of USA', 'PA': 'East of USA', 'NJ': 'East of USA',
        'CT': 'East of USA', 'RI': 'East of USA', 'DE': 'East of USA', 'MD': 'East of USA',
        'DC': 'East of USA', 'OH': 'East of USA', 'WV': 'East of USA', 'VA': 'East of USA',
        'NC': 'East of USA', 'KY': 'East of USA',
        'TX': 'South of USA', 'FL': 'South of USA', 'GA': 'South of USA', 'AL': 'South of USA',
        'SC': 'South of USA', 'TN': 'South of USA', 'MS': 'South of USA', 'LA': 'South of USA',
        'AR': 'South of USA', 'NM': 'South of USA',
        'PR': 'Caribbean'
    }
    df['Customer Region'] = df['Customer State'].map(state_to_region)

    df_customer = df[
        ['Customer Country', 'Customer Region', 'Customer State', 'Customer City', 'Customer Zipcode']].copy()
    df_customer.columns = ['country', 'region', 'state', 'city', 'zip_code']  # Standardize column names

    df_order = df[['Order Country', 'Order Region', 'Order State', 'Order City', 'Order Zipcode']].copy()
    df_order.columns = ['country', 'region', 'state', 'city', 'zip_code']  # Standardize column names

    # Concatenate the customer and order addresses
    location_df = pd.concat([df_customer, df_order], axis=0)  # Stack rows

    location_df["zip_code"] = location_df["zip_code"].apply(clean_zipcode)  # Clean up ZIP codes

    # Drop duplicates to create a unique address DataFrame
    location_df = location_df.drop_duplicates().reset_index(drop=True)

    location_df.to_sql(name="locations",
                       con=engine,
                       if_exists="append",
                       index=False,
                       )
    set_data_created(engine, "locations")


def create_customers(df: pd.DataFrame, engine: sqlalchemy.engine.Engine):
    if data_already_created(engine, "customers"):
        return
    customers = df.drop_duplicates(subset=["Customer Id"], keep="last")[[
        "Customer Id",
        "Customer Fname",
        "Customer Lname",
        "Customer Segment",
        "Market",
        "Order Country",
        "Order Region",
        "Order State",
        "Order City",
        "Order Zipcode"
    ]].rename(columns={
        "Customer Id": "customer_id",
        "Customer Fname": "first_name",
        "Customer Lname": "last_name",
        "Customer Segment": "segment",
        "Market": "market",
        "Order Country": "country",
        "Order Region": "region",
        "Order State": "state",
        "Order City": "city",
        "Order Zipcode": "zip_code"
    })
    customers["zip_code"] = customers["zip_code"].apply(clean_zipcode)

    # Add LocationID by matching existing locations or inserting new ones
    with engine.connect() as connection:
        customers["location_id"] = customers.apply(lambda row: get_or_create_location(row, connection), axis=1)

    # Save the customer data to the database
    customers = customers[["customer_id", "first_name", "last_name", "segment", "market", "location_id"]]
    customers.to_sql(name="customers",
                     con=engine,
                     if_exists="append",
                     index=False)
    set_data_created(engine, "customers")


def get_or_create_location(row, connection):
    select_query = sqlalchemy.text("""
        SELECT location_id 
        FROM locations 
        WHERE zip_code = :zipcode 
          AND city = :city 
          AND state = :state 
          AND country = :country 
          AND region IS NOT DISTINCT FROM :region
    """)

    # Execute the query with parameters
    result = connection.execute(select_query, {
        "zipcode": row["zip_code"],
        "city": row["city"],
        "state": row["state"],
        "country": row["country"],
        "region": row["region"]
    }).fetchone()

    # If location exists, return its LocationID
    if result:
        return result[0]

    # If not found, insert a new location
    insert_query = sqlalchemy.text("""
        INSERT INTO locations (zip_code, city, state, country, region)
        VALUES (:zipcode, :city, :state, :country, :region)
        RETURNING location_id
    """)

    # Execute the insert query and return the new LocationID
    result = connection.execute(insert_query, {
        "zipcode": row["zip_code"],
        "city": row["city"],
        "state": row["state"],
        "country": row["country"],
        "region": row["region"]
    }).fetchone()

    return result[0]


def clean_zipcode(zipcode):
    if pd.isna(zipcode):
        return ''
    return str(int(zipcode))


def data_already_created(engine: sqlalchemy.engine.Engine, creator: str) -> bool:
    with engine.connect() as conn:
        created = conn.execute(sqlalchemy.text('SELECT created FROM data_gen WHERE module LIKE :creator'),
                               {"creator": creator}).fetchall()
    if not created:
        return False
    return created[0][0]


def set_data_created(engine: sqlalchemy.engine.Engine, creator: str, value: bool = True):
    with engine.connect() as conn:
        res = conn.execute(sqlalchemy.text('INSERT into data_gen (module, created) '
                                           'VALUES (:creator, :value) '
                                           'ON CONFLICT (module)'
                                           'DO UPDATE SET '
                                           'created = :value'),
                           {"creator": creator, "value": value})
        conn.commit()
