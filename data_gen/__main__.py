import pandas as pd
import os

from sqlalchemy import create_engine

from data_gen import POSTGRES_CONNECTION
from data_gen.create_initial_data import create_products, create_locations, create_customers, create_retailers
from data_gen.kafka_producer import event_generation_loop, create_kafka_topics

dirname = os.path.dirname(__file__)


def main():
    df = pd.read_csv(dirname + "/data/DataCoSupplyChainDataset.csv", encoding="ISO-8859-1")
    engine = create_engine(POSTGRES_CONNECTION)

    create_products(df, engine)
    create_locations(df, engine)
    create_customers(df, engine)
    create_retailers(df, engine)

    create_kafka_topics()
    event_generation_loop(df, engine)


if __name__ == '__main__':
    main()
