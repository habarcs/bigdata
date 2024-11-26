import faker
import pandas as pd
import sqlalchemy


def create_products(df: pd.DataFrame, engine: sqlalchemy.engine.Engine):
    products = df.drop_duplicates(subset="Product Card Id", keep="first")[[
        "Product Card Id",
        "Product Name",
        "Category Name",
        "Product Price"
    ]].rename(columns={
        "Product Card Id": "ProductID",
        "Product Name": "ProductName",
        "Category Name": "Category",
        "Product Price": "ProductPrice"
    })
    products.to_sql(name="Products",
                    con=engine,
                    if_exists="append",
                    index=False,
                    index_label="ProductID"
                    )


def create_retailers(df: pd.DataFrame, engine: sqlalchemy.engine.Engine):
    fake = faker.Faker()
    retailers = df.drop_duplicates(subset=["Customer City", "Customer State", "Customer Country"], keep="first")[[
        "Customer Country",
        "Customer State",
        "Customer City",
    ]].rename(columns={
        "Customer Country": "RetailerCountry",
        "Customer State": "RetailerState",
        "Customer City": "RetailerCity"
    })
    retailers["RetailerName"] = [fake.company() for _ in retailers.index]

    retailers.to_sql(name="Retailers",
                     con=engine,
                     if_exists="append",
                     index=False)


def create_locations(df: pd.DataFrame, engine: sqlalchemy.engine.Engine):
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
    df_customer.columns = ['Country', 'Region', 'State', 'City', 'ZipCode']  # Standardize column names

    df_order = df[['Order Country', 'Order Region', 'Order State', 'Order City', 'Order Zipcode']].copy()
    df_order.columns = ['Country', 'Region', 'State', 'City', 'ZipCode']  # Standardize column names

    # Concatenate the customer and order addresses
    location_df = pd.concat([df_customer, df_order], axis=0)  # Stack rows

    location_df["ZipCode"] = location_df["ZipCode"].apply(clean_zipcode)  # Clean up ZIP codes

    # Drop duplicates to create a unique address DataFrame
    location_df = location_df.drop_duplicates().reset_index(drop=True)

    location_df.to_sql(name="Locations",
                       con=engine,
                       if_exists="append",
                       index=False,
                       )


def create_customers(df: pd.DataFrame, engine: sqlalchemy.engine.Engine):
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
        "Customer Id": "CustomerID",
        "Customer Fname": "FirstName",
        "Customer Lname": "LastName",
        "Customer Segment": "Segment",
        "Market": "Market",
        "Order Country": "Country",
        "Order Region": "Region",
        "Order State": "State",
        "Order City": "City",
        "Order Zipcode": "ZipCode"
    })
    customers["ZipCode"] = customers["ZipCode"].apply(clean_zipcode)

    # Add LocationID by matching existing locations or inserting new ones
    with engine.connect() as connection:
        customers["LocationID"] = customers.apply(lambda row: get_or_create_location(row, connection), axis=1)

    # Save the customer data to the database
    customers = customers[["CustomerID", "FirstName", "LastName", "Segment", "Market", "LocationID"]]
    customers.to_sql(name="Customers",
                     con=engine,
                     if_exists="append",
                     index=False)


def get_or_create_location(row, connection):
    select_query = sqlalchemy.text("""
        SELECT "LocationID" 
        FROM "Locations" 
        WHERE "ZipCode" = :zipcode 
          AND "City" = :city 
          AND "State" = :state 
          AND "Country" = :country 
          AND "Region" IS NOT DISTINCT FROM :region
    """)

    # Execute the query with parameters
    result = connection.execute(select_query, {
        "zipcode": row["ZipCode"],
        "city": row["City"],
        "state": row["State"],
        "country": row["Country"],
        "region": row["Region"]
    }).fetchone()

    # If location exists, return its LocationID
    if result:
        return result[0]

    # If not found, insert a new location
    insert_query = sqlalchemy.text("""
        INSERT INTO "Locations" ("ZipCode", "City", "State", "Country", "Region")
        VALUES (:zipcode, :city, :state, :country, :region)
        RETURNING "LocationID"
    """)

    # Execute the insert query and return the new LocationID
    result = connection.execute(insert_query, {
        "zipcode": row["ZipCode"],
        "city": row["City"],
        "state": row["State"],
        "country": row["Country"],
        "region": row["Region"]
    }).fetchone()

    return result[0]


def clean_zipcode(zipcode):
    if pd.isna(zipcode):
        return ''
    return str(int(zipcode))
