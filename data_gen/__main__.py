import pandas as pd
import os

from sqlalchemy import create_engine

from data_gen import POSTGRES_CONNECTION
from data_gen.create_initial_data import create_products, create_locations, create_customers

dirname = os.path.dirname(__file__)

def main():
    df = pd.read_csv(dirname + "/data/DataCoSupplyChainDataset.csv", encoding="ISO-8859-1")
    engine = create_engine(POSTGRES_CONNECTION)
    create_products(df, engine)
    create_locations(df, engine)
    create_customers(df, engine)

if __name__ == '__main__':
    main()
