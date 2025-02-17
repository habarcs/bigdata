# import logging
import os
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine

# logging.basicConfig()
# logging.getLogger("sqlalchemy.engine").setLevel(logging.DEBUG)

from data_gen import POSTGRES_CONNECTION
from data_gen.create_initial_data import create_products, create_locations, create_customers, create_retailers, \
    create_inventory
from data_gen.kafka_producer import event_generation_loop, create_kafka_topics

dirname = os.path.dirname(__file__)


def main():
    df = pd.read_csv(dirname + "/data/DataCoSupplyChainDataset.csv", encoding="ISO-8859-1")
    engine = create_engine(POSTGRES_CONNECTION)

    # using the dataset create the initial records in the database
    create_products(df, engine)
    create_locations(df, engine)
    create_customers(df, engine)
    create_retailers(df, engine)
    create_inventory(df, engine)

    # create kafka topics where the orders will actually be sent
    create_kafka_topics()
    # signify to other docker containers that data generation is done
    Path("/run/produce.ready").touch()
    # start generating events in the order topic
    event_generation_loop(df, engine)


if __name__ == '__main__':
    main()
