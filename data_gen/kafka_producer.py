import json
import signal
import time

import pandas as pd
import sqlalchemy
from confluent_kafka import Producer, KafkaError, KafkaException
from confluent_kafka.admin import AdminClient, NewTopic

from data_gen import KAFKA_CONF


def create_kafka_topics():
    admin_client = AdminClient(KAFKA_CONF)
    new_topics = [
        NewTopic("orders")
    ]
    fs = admin_client.create_topics(new_topics)
    for topic, f in fs.items():
        try:
            f.result()
            print(f"Topic {topic} created")
        except KafkaException as e:
            if e.args[0].code() == KafkaError.TOPIC_ALREADY_EXISTS:
                print(f"Topic {topic} already exists")
            else:
                print(f"Failed to create topic {topic}: {e}")
                raise
        except Exception as e:
            print(f"Failed to create topic {topic}: {e}")
            raise


def acked(err, msg):
    if err is not None:
        print("Failed to deliver message: %s: %s" % (str(msg), str(err)))
    else:
        print("Message produced: %s" % (str(msg)))


def order_generator(df: pd.DataFrame, engine: sqlalchemy.engine.Engine):
    df = df.assign(
        order_date=pd.to_datetime(df['order date (DateOrders)'], format="%m/%d/%Y %H:%M", errors="coerce")
    ).sort_values(by='order_date', ascending=True).drop(columns=['order_date']).reset_index(drop=True)
    
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
        "Type": "transaction_type",
        "Days for shipping (real)": "real_shipping_days",
        "Days for shipment (scheduled)": "scheduled_shipping_days",
        "Delivery Status": "delivery_status",
        "Late_delivery_risk": "late_risk",
        "order date (DateOrders)": "order_date",
        "Order Id": "order_id",
        "Product Card Id": "product_id",
        "Order Item Quantity": "item_quantity",
        "Order Status": "status",
        "shipping date (DateOrders)": "shipping_data",
        "Shipping Mode": "shipping_mode",
        "Customer Id": "customer_id",
    })

    retailers = df[[
        "Customer City",
        "Customer State",
        "Customer Country"
    ]].rename(columns={
        "Customer City": "retailer_city",
        "Customer State": "retailer_state",
        "Customer Country": "retailer_country"
    })

    for order, retailer in zip(orders.itertuples(), retailers.itertuples()):
        with engine.connect() as conn:
            retailer_id = conn.execute(sqlalchemy.text(
                'SELECT retailer_id from retailers '
                'WHERE retailer_city LIKE :retailer_city '
                'AND retailer_state LIKE :retailer_state '
                'AND retailer_country LIKE :retailer_country'),
                {
                    "retailer_city": retailer.retailer_city,
                    "retailer_state": retailer.retailer_state,
                    "retailer_country": retailer.retailer_country
                }).fetchone()[0]
            order_dict = order._asdict()
            order_dict["retailer_id"] = retailer_id
            del order_dict["Index"]
        yield order_dict


def send_event(producer: Producer, order: dict):
    key = str(order["order_id"])
    producer.poll(0)
    producer.produce("orders", key=key, value=json.dumps(order), callback=acked)


def event_generation_loop(df: pd.DataFrame, engine: sqlalchemy.engine.Engine):
    producer = Producer(KAFKA_CONF)

    def signal_handler(_, __):
        producer.flush()
        print("Stopping run, exiting.")
        exit(0)

    signal.signal(signal.SIGTERM, signal_handler)

    for order in order_generator(df, engine):
        send_event(producer, order)
        time.sleep(0.1)
    producer.flush()
