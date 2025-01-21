import pandas as pd
from sqlalchemy import create_engine

DATABASE_URL = "postgresql+psycopg2://postgres:supersecret@localhost:5432/postgres"

def get_engine():
    """
    Creates a SQLAlchemy engine for database interaction.
    """
    return create_engine(DATABASE_URL)

def load_static_data():
    """
    Loads retailers and products data from the database.
    """
    engine = get_engine()
    retailers = pd.read_sql("SELECT * FROM retailers", con=engine)
    products = pd.read_sql("SELECT * FROM products", con=engine)
    return retailers, products
