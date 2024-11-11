import random
import time
import uuid
import json
import sched

import numpy as np
import psycopg
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka import Producer
from faker import Faker

from data_gen import KAFKA_CONF, POSTGRES_CONNECTION


def create_kafka_topics():
    admin_client = AdminClient(KAFKA_CONF)
    new_topics = [
        NewTopic("Orders"), NewTopic("SuppliedMaterial"), NewTopic("Shipping"), NewTopic("ManufacturedProducts")
    ]
    fs = admin_client.create_topics(new_topics)
    for topic, f in fs.items():
        try:
            f.result()
            print(f"Topic {topic} created")
        except Exception as e:
            print(f"Failed to create topic {topic}: {e}")


def generate_order_and_shipping_event(fake: Faker):
    # TODO fix event generation so it is more consistent with data we already have and it makes sense in real world
    conn: psycopg.Connection
    with psycopg.connect(POSTGRES_CONNECTION) as conn:
        cur: psycopg.Cursor
        with conn.cursor() as cur:
            customer_ids = [row[0] for row in cur.execute("SELECT CustomerID from Customers").fetchall()]
            retailer_ids = [row[0] for row in cur.execute("SELECT RetailerID from Retailers").fetchall()]
            product_ids = [row[0] for row in cur.execute("SELECT ProductID from Products").fetchall()]

    order_id = str(uuid.uuid4())  # we generate an uuid randomly, the chance of collision is very low, see https://en.wikipedia.org/wiki/Universally_unique_identifier#Collisions
    shipping_options = ["Shipped", "Delivered", "In Transit", "Out for Delivery", "Awaiting Pickup"]
    payment_options = ["Card", "Gift Card", "Cash", "Cryptocurrency", "Apple Pay", "Google Pay", "Spices", "Gold Coins",
                       "Klarna"]

    # Mean discount 8 maximum 100 percent, the chance of having a discount is 30 percent
    discount = max((np.random.poisson(7) + 1), 100) if random.random() < 0.3 else 0
    customer_id = random.choice(customer_ids)
    current_time = int(time.time())

    # this part should be more coherent so we can run predictions on it
    # total amount should be calculated from product prices that have to be queried from database, discount should be
    # then applied, profit should be the same
    order = {
        'OrderID': order_id,
        'CustomerID': customer_id,
        'OrderDate': current_time,
        'TotalAmount': round(random.uniform(5, 100), 2),
        'OrderStatus': random.choice(shipping_options),
        'PaymentMethod': random.choice(payment_options),
        'ProductID': random.sample(product_ids, np.random.poisson(2) + 1),
        'OrderItemDiscount': discount,
        'OrderItemTotal': round(random.uniform(1, 1000), 3),
        'OrderProfitPerOrder': round(random.uniform(0, 1), 3),
        'OrderQuantity': round(random.uniform(1, 1000)),
        'OrderCity': fake.city(),
        'OrderState': fake.state(),
        'OrderCountry': fake.country(),
        'ShippingID': order_id,
        'RetailerID': random.choice(retailer_ids)
    }

    shippers = ["DHL", "FedEx", "UPS", "USPS (United States Postal Service)", "TNT", "Royal Mail", "Canada Post",
                "Australia Post", "Maersk", "CMA CGM", "XPO Logistics", "China Post", "Japan Post", "La Poste",
                "India Post", "Singapore Post (SingPost)", "Aramex", "PostNL", "YRC Worldwide", "Seur"]
    payment_status = ["Pending", "Paid", "Overdue", "Cancelled", "Refunded", "Failed", "Processing", "Authorized",
                      "Declined"]

    shipping = {
        'OrderID': order_id,
        'ShippingID': order_id,
        'CustomerID': customer_id,
        'Address': fake.address(),  # should be queried from database
        'Shipper': random.choice(shippers),
        'TrackingNumber': str(uuid.uuid4()),
        'ShippingDate': current_time,
        'EstimatedDeliveryDate': current_time + random.randint(3, 10) * 86400,
        'ActualDeliveryDate': current_time + random.randint(3, 10) * 86400,
        # should be null if shipment is not yet delivered
        'ShipmentStatus': random.choice(shipping_options),
        'Weight': random.uniform(5, 100),
        'Dimensions': (random.uniform(10, 500), 2),
        'Payment_status': random.choice(payment_status),
        'Origin': fake.country(),
        'Destination': fake.country()
    }
    return order, shipping


def acked(err, msg):
    if err is not None:
        print("Failed to deliver message: %s: %s" % (str(msg), str(err)))
    else:
        print("Message produced: %s" % (str(msg)))


def gen_and_send_event(fake: Faker, producer: Producer):
    order, shipping = generate_order_and_shipping_event(fake)
    key = order["OrderID"]
    producer.produce("Orders", key=key, value=json.dumps(order), callback=acked)
    producer.produce("Shipping", key=key, value=json.dumps(shipping), callback=acked)
    producer.poll(1)


def event_generation_loop(fake: Faker):
    producer = Producer(KAFKA_CONF)
    scheduler = sched.scheduler(time.time, time.sleep)

    def repeat_task():
        scheduler.enter(2, 1, gen_and_send_event, (fake, producer))
        scheduler.enter(2, 1, repeat_task, ())

    repeat_task()
    scheduler.run()
