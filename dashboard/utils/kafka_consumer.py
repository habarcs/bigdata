import pandas as pd
from kafka import KafkaConsumer
import json
import time
from dotenv import load_dotenv

load_dotenv()
import os

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
KAFKA_TOPIC = "orders"


def consume_kafka_data():
    """
    Consumes all available data from Kafka until no new data is received for 5 seconds.
    """
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        auto_offset_reset="earliest",
        enable_auto_commit=True,
    )
    orders = []
    start_time = time.time()

    while True:
        message = consumer.poll(timeout_ms=1000)  # Wait 1 second for new messages
        if message:
            for topic_partition, messages in message.items():
                for msg in messages:
                    orders.append(msg.value)
        else:
            if time.time() - start_time > 5:  # Stop if no new messages for 5 seconds
                break

    consumer.close()
    return pd.DataFrame(orders)


def kafka_realtime_consumer():
    """
    Consumes all available data from Kafka until no new data is received for 5 seconds.
    """
    
    # Kafka Consumer setup
    consumer = KafkaConsumer(
        'orders',
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        auto_offset_reset='latest',
        enable_auto_commit=True,
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    
    return consumer
    
    