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
