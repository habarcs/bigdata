import pandas as pd
import sqlalchemy
import faker


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
