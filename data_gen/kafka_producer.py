import json
import signal
import time

import pandas as pd
import sqlalchemy
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

from data_gen import KAFKA_CONF


def create_kafka_topics():
    admin_client = AdminClient(KAFKA_CONF)
    new_topics = [
        NewTopic("Orders")
    ]
    fs = admin_client.create_topics(new_topics)
    for topic, f in fs.items():
        try:
            f.result()
            print(f"Topic {topic} created")
        except Exception as e:
            print(f"Failed to create topic {topic}: {e}")


def acked(err, msg):
    if err is not None:
        print("Failed to deliver message: %s: %s" % (str(msg), str(err)))
    else:
        print("Message produced: %s" % (str(msg)))


def order_generator(df: pd.DataFrame, engine: sqlalchemy.engine.Engine):
    orders = df[["Type",
                 "Days for shipping (real)",
                 "Days for shipment (scheduled)",
                 "Delivery Status",
                 "Late_delivery_risk",
                 "order date (DateOrders)",
                 "Order Id",
                 "Product Card Id",
                 "Order Item Quantity",
                 "Order Status",
                 "shipping date (DateOrders)",
                 "Shipping Mode",
                 "Customer Id",
                 ]].rename(columns={
        "Type": "TransactionType",
        "Days for shipping (real)": "RealShippingDays",
        "Days for shipment (scheduled)": "ScheduledShippingDays",
        "Delivery Status": "DeliveryStatus",
        "Late_delivery_risk": "LateRisk",
        "order date (DateOrders)": "OrderDate",
        "Order Id": "OrderID",
        "Product Card Id": "ProductID",
        "Order Item Quantity": "ItemQuantity",
        "Order Status": "Status",
        "shipping date (DateOrders)": "ShippingData",
        "Shipping Mode": "ShippingMode",
        "Customer Id": "CustomerID",
    })
    retailers = df[[
        "Customer City",
        "Customer State",
        "Customer Country"
    ]].rename(columns={
        "Customer City": "RetailerCity",
        "Customer State": "RetailerState",
        "Customer Country": "RetailerCountry"
    })

    for order, retailer in zip(orders.itertuples(), retailers.itertuples()):
        with engine.connect() as conn:
            retailer_id = conn.execute(sqlalchemy.text(
                'SELECT RetailerID from Retailers '
                'WHERE RetailerCity LIKE :retailer_city '
                'AND RetailerState LIKE :retailer_state '
                'AND RetailerCountry LIKE :retailer_country'),
                {
                    "retailer_city": retailer.RetailerCity,
                    "retailer_state": retailer.RetailerState,
                    "retailer_country": retailer.RetailerCountry
                }).fetchone()[0]
            order_dict = order._asdict()
            order_dict["RetailerID"] = retailer_id
            del order_dict["Index"]
        yield order_dict


def send_event(producer: Producer, order: dict):
    key = str(order["OrderID"])
    producer.poll(0)
    producer.produce("Orders", key=key, value=json.dumps(order), callback=acked)


def event_generation_loop(df: pd.DataFrame, engine: sqlalchemy.engine.Engine):
    producer = Producer(KAFKA_CONF)

    def signal_handler(_, __):
        producer.flush()
        print("Stopping run, exiting.")
        exit(0)

    signal.signal(signal.SIGTERM, signal_handler)

    for order in order_generator(df, engine):
        send_event(producer, order)
        time.sleep(1)
    producer.flush()
