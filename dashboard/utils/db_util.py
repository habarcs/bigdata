import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv

load_dotenv()
import os

SQL_ADDRESS = os.getenv("SQL_ADDRESS")

def get_engine():
    """
    Creates a SQLAlchemy engine for database interaction.
    """
    return create_engine(f"postgresql+psycopg2://postgres:supersecret@{SQL_ADDRESS}:5432/postgres")


def load_static_data():
    """
    Loads retailers and products data from the database.
    """
    engine = get_engine()
    retailers = pd.read_sql("SELECT * FROM retailers", con=engine)
    products = pd.read_sql("SELECT * FROM products", con=engine)
    return retailers, products
