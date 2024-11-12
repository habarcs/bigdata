
import random
import time
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
import socket
from faker import Faker
import psycopg
from data_gen.create_initial_data import POSTGRES_CONNECTION
import numpy as np


CONF = {'bootstrap.servers': 'localhost:9092', #change later
        'client.id': socket.gethostname()}


def create_kafka_topics():
    admin_client = AdminClient(CONF)
    new_topics = [
        NewTopic("Orders"), NewTopic("SuppliedMaterial"), NewTopic("Shipping"), NewTopic("manufacturedProducts")
    ]
    fs = admin_client.create_topics(new_topics)
    for topic, f in fs.items():
        try:
            f.result()
            print(f"Topic {topic} created")
        except Exception as e:
            print(f"Failed to create topic {topic}: {e}")




def generate_order_event(order_id, fake: Faker):
    connections = set()  # no duplicates
    conn: psycopg.Connection
    with psycopg.connect(POSTGRES_CONNECTION) as conn:
        cur: psycopg.Cursor
        with conn.cursor() as cur:
            customer_ids = [row[0] for row in cur.execute("SELECT CustumerID from Customers").fetchall()]
            retailer_ids= [row[0] for row in cur.execute("SELECT RetailerID from Retailers").fetchall()]
            product_ids= [row[0] for row in cur.execute("SELECT ProductID from Products").fetchall()]
            
    shipping_options = ["Shipped", "Delivered", "In Transit", "Out for Delivery", "Awaiting Pickup"]
    payment_options = ["Card", "Gift Card", "Cash", "Cryptocurrency", "Apple Pay", "Google Pay", "Spices", "Gold Coins", "Klarna" ]
    discount = random.gauss(8, 5) # Mean discount 8%, with +- 5

    
    order = {
        'OrderID': order_id,
        'CustomerID': random.choice(customer_ids),
        'OrderDate': int(time.time()),
        'TotalAmount': round(random.uniform(5, 100), 2),
        'OrderStatus': random.choice(shipping_options), 
        'PaymentMethod': random.choice(payment_options),
        'Product ID': random.sample(product_ids, np.random.poisson(2) + 1),
        'OrderItemDiscount': abs(round(discount)) if random.random() < 0.3 else 0,
        'OrderItemTotal': round(random.uniform(1, 1000), 3),
        'OrderProfitPerOrder': round(random.uniform(0, 1), 3),
        'OrderQuantity': round(random.uniform(1, 1000)),
        'OrderCity': fake.city(),
        'OrderState': fake.state(),
        'OrderCountry': fake.country(),
        'ShippingID': order_id,
        'RetailerID': random.choice(retailer_ids)
    }
    return order


def generate_shipping_event(shipping_id, fake: Faker):
    connections = set()  # no duplicates
    conn: psycopg.Connection
    with psycopg.connect(POSTGRES_CONNECTION) as conn:
        cur: psycopg.Cursor
        with conn.cursor() as cur:
            customer_ids = [row[0] for row in cur.execute("SELECT CustumerID from Customers").fetchall()]
            address_customer = [row[0] for row in cur.execute("SELECT Address from Customers").fetchall()]
            country_custumer = [row[0] for row in cur.execute("SELECT Country from Customers").fetchall()]
      
    shipping_options = ["Shipped", "Delivered", "In Transit", "Out for Delivery", "Awaiting Pickup"]                    
    shippers = ["DHL", "FedEx", "UPS", "USPS (United States Postal Service)", "TNT", "Royal Mail", "Canada Post", "Australia Post", "Maersk", "CMA CGM", "XPO Logistics", "China Post", "Japan Post", "La Poste", "India Post", "Singapore Post (SingPost)", "Aramex", "PostNL", "YRC Worldwide", "Seur"]
    payment_status = ["Pending", "Paid", "Overdue", "Cancelled", "Refunded", "Failed", "Processing", "Authorized", "Declined"]

    current_time = int(time.time())
    
    shipping = {

        'order_id' = order_id,
        'customer_id' = customer_name,
        'address' = address_customer,
        'shipper' = random.choice(shippers),
        #tracking_number = tracking_number # what to do with this one?
        'shipping_date' = current_time,
        'estimated_delivery_date' = current_time + random.randint(3, 10) * 86400,
        'actual_delivery_date' = current_time + random.randint(3, 10) * 86400,
        'shipment_status' = random.choice(shipping_options),
        'weight' = random.uniform((5, 100), 2),
        'dimensions' = (random.uniform(10, 500), 2),
        'payment_status' = random.choice(payment_status),
        'origin' = fake.country(),
        'destination' = country_custumer
    }
    return order




