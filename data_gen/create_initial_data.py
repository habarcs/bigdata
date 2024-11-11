from faker import Faker
import psycopg
import os
import random
from __init__ import geo_area_provider, store_type_provider
from kafka_producer import create_kafka_topics, list_kafka_topics

# TODO refer to the postgres host by sql-database once this code runs in docker
# TODO less important: secret should not be clear
POSTGRES_CONNECTION = "dbname=postgres user=postgres host=localhost port=5432 password=supersecret"

NUM_SUPPLIERS = int(os.getenv("NUM_SUPPLIERS", 200))
NUM_MANUFACTURERS = int(os.getenv("NUM_MANUFACTURERS", 30))
NUM_SUP_MAN_CON = int(os.getenv("NUM_SUPPLIERS", 300))  # this should be higher than NUM_SUPPLIERS and NUM_MANUFACTURERS
NUM_PRODUCTS = int(os.getenv("NUM_PRODUCTS", 100))
NUM_DISTRIBUTORS = int(os.getenv("NUM_DISTRIBUTORS", 70))
NUM_RETAILERS = int(os.getenv("NUM_RETAILERS", 38))
NUM_CUSTOMERS = int(os.getenv("NUM_CUSTOMERS", 666))


def create_suppliers(fake: Faker):
    supplier_names = [fake.unique.company() for _ in range(NUM_SUPPLIERS)]
    suppliers = []
    for i in range(NUM_SUPPLIERS):
        supplier = (
            supplier_names[i],  # SupplierName
            fake.name(),  # ContactName
            fake.email(),  # ContactEmail
            fake.phone_number(),  # ContactPhone
            fake.address(),  # Address
            fake.city(),  # City
            fake.state(),  # State
            fake.zipcode(),  # ZipCode
            fake.country(),  # Country
            random.randint(1, 10),  # Rating
        )
        suppliers.append(supplier)

    conn: psycopg.Connection
    with psycopg.connect(POSTGRES_CONNECTION, autocommit=True) as conn:
        cur: psycopg.Cursor
        with conn.cursor() as cur:
            copy: psycopg.Copy
            with cur.copy(
                    "COPY Suppliers (SupplierName, ContactName, ContactEmail, ContactPhone, Address,"
                    " City, State, ZipCode, Country, Rating) FROM STDIN") as copy:
                for supplier in suppliers:
                    copy.write_row(supplier)


def create_manufacturers(fake: Faker):
    manufacturer_names = [fake.unique.company() for _ in range(NUM_MANUFACTURERS)]
    manufacturers = []
    for i in range(NUM_MANUFACTURERS):
        manufacturer = (
            manufacturer_names[i],  # ManufacturerName
            fake.name(),  # ContactName
            fake.email(),  # ContactEmail
            fake.phone_number(),  # ContactPhone
            fake.address(),  # Address
            fake.city(),  # City
            fake.state(),  # State
            fake.zipcode(),  # ZipCode
            fake.country(),  # Country
            random.randint(100, 100000),  # ProductionCapacity
        )
        manufacturers.append(manufacturer)

    conn: psycopg.Connection
    with psycopg.connect(POSTGRES_CONNECTION, autocommit=True) as conn:
        cur: psycopg.Cursor
        with conn.cursor() as cur:
            copy: psycopg.Copy
            with cur.copy(
                    "COPY Manufacturers (ManufacturerName, ContactName, ContactEmail, ContactPhone, Address,"
                    " City, State, ZipCode, Country, ProductionCapacity) FROM STDIN") as copy:
                for manufacturer in manufacturers:
                    copy.write_row(manufacturer)


def create_supplier_manufacturer_conn():
    connections = set()  # no duplicates
    conn: psycopg.Connection
    with psycopg.connect(POSTGRES_CONNECTION) as conn:
        cur: psycopg.Cursor
        with conn.cursor() as cur:
            supplier_ids = [row[0] for row in cur.execute("SELECT SupplierID from Suppliers").fetchall()]
            manufacturer_ids = [row[0] for row in cur.execute("SELECT ManufacturerID from Manufacturers").fetchall()]

        # make sure every supplier has at least one contact and vice versa
        for supplier_id in supplier_ids:
            connections.add((supplier_id, random.choice(manufacturer_ids)))
        for manufacturer_id in manufacturer_ids:
            connections.add((random.choice(supplier_ids), manufacturer_id))
        while len(connections) < NUM_SUP_MAN_CON:
            connections.add((random.choice(supplier_ids), random.choice(manufacturer_ids)))
        connections = list(connections)

        cur: psycopg.Cursor
        with conn.cursor() as cur:
            copy: psycopg.Copy
            with cur.copy(
                    "COPY Supplier_Manufacturer (SupplierID, ManufacturerID) FROM STDIN") as copy:
                for connection in connections:
                    copy.write_row(connection)


def create_products(fake: Faker):
    products = []

    # Fetch manufacturer IDs to associate with products
    conn: psycopg.Connection
    with psycopg.connect(POSTGRES_CONNECTION) as conn:
        cur: psycopg.Cursor
        with conn.cursor() as cur:
            manufacturer_ids = [row[0] for row in cur.execute("SELECT ManufacturerID FROM Manufacturers").fetchall()]

        for _ in range(NUM_PRODUCTS):
            product = (
                fake.unique.word(),  # ProductName
                fake.word(ext_word_list=['Electronics', 'Furniture', 'Toys', 'Tools', 'Apparel']),  # Category
                fake.sentence(nb_words=10),  # Description
                round(random.uniform(10.0, 1000.0), 2),  # UnitPrice
                random.choice(manufacturer_ids) if manufacturer_ids else None,  # ManufacturerID
                random.choice(['1 year', '2 years', '3 years'])  # WarrantyPeriod
            )
            products.append(product)

        cur: psycopg.Cursor
        with conn.cursor() as cur:
            copy: psycopg.Copy
            with cur.copy("COPY Products (ProductName, Category, Description, UnitPrice, ManufacturerID,"
                          " WarrantyPeriod) FROM STDIN") as copy:
                for product in products:
                    copy.write_row(product)


def create_inventory(fake: Faker):
    conn: psycopg.Connection
    with psycopg.connect(POSTGRES_CONNECTION) as conn:
        cur: psycopg.Cursor
        with conn.cursor() as cur:
            product_ids = [row[0] for row in cur.execute("SELECT ProductID FROM Products").fetchall()]

        inventory_data = []
        for product_id in product_ids:
            inventory = (
                product_id,  # ProductID
                random.randint(0, 500),  # QuantityOnHand, randomly between 0 and 500
                random.randint(10, 50),  # ReorderLevel, randomly between 10 and 50
                fake.city()  # Location, using a random city name
            )
            inventory_data.append(inventory)

        cur: psycopg.Cursor
        with conn.cursor() as cur:
            copy: psycopg.Copy
            with cur.copy("COPY Inventory (ProductID, QuantityOnHand, ReorderLevel, Location) FROM STDIN") as copy:
                for inv in inventory_data:
                    copy.write_row(inv)


def create_distributors(fake: Faker):
    distributor_names = [fake.unique.company() for _ in range(NUM_DISTRIBUTORS)]
    distributors = []

    for i in range(NUM_DISTRIBUTORS):
        distributor = (
            distributor_names[i],  # DistributorName
            fake.name(),  # ContactName
            fake.email(),  # ContactEmail
            fake.phone_number(),  # ContactPhone
            fake.address(),  # Address
            fake.city(),  # City
            fake.state(),  # State
            fake.zipcode(),  # ZipCode
            fake.country(),  # Country
            fake.geo_area(),  # DistributionArea, a general geographic area
        )
        distributors.append(distributor)

    # Insert data into Distributors table
    conn: psycopg.Connection
    with psycopg.connect(POSTGRES_CONNECTION) as conn:
        cur: psycopg.Cursor
        with conn.cursor() as cur:
            copy: psycopg.Copy
            with cur.copy("COPY Distributors (DistributorName, ContactName, ContactEmail, ContactPhone, "
                          "Address, City, State, ZipCode, Country, DistributionArea) FROM STDIN") as copy:
                for distributor in distributors:
                    copy.write_row(distributor)


def create_retailers(fake: Faker):
    retailer_names = [fake.unique.company() for _ in range(NUM_RETAILERS)]
    retailers = []

    for i in range(NUM_RETAILERS):
        retailer = (
            retailer_names[i],  # RetailerName
            fake.name(),  # ContactName
            fake.email(),  # ContactEmail
            fake.phone_number(),  # ContactPhone
            fake.address(),  # Address
            fake.city(),  # City
            fake.state(),  # State
            fake.zipcode(),  # ZipCode
            fake.country(),  # Country
            fake.store_type(),  # StoreType
        )
        retailers.append(retailer)

    # Insert data into Retailers table
    conn: psycopg.Connection
    with psycopg.connect(POSTGRES_CONNECTION) as conn:
        cur: psycopg.Cursor
        with conn.cursor() as cur:
            copy: psycopg.Copy
            with cur.copy("COPY Retailers (RetailerName, ContactName, ContactEmail, ContactPhone, "
                          "Address, City, State, ZipCode, Country, StoreType) FROM STDIN") as copy:
                for retailer in retailers:
                    copy.write_row(retailer)


def create_customers(fake: Faker):
    customers = []

    for _ in range(NUM_CUSTOMERS):
        customer = (
            fake.first_name(),  # FirstName
            fake.last_name(),  # LastName
            fake.email(),  # Email
            fake.phone_number(),  # Phone
            fake.address(),  # Address
            fake.city(),  # City
            fake.state(),  # State
            fake.zipcode(),  # ZipCode
            fake.country(),  # Country
            random.randint(0, 1000),  # LoyaltyPoints (random number between 0 and 1000)
        )
        customers.append(customer)

    # Insert data into Customers table
    conn: psycopg.Connection
    with psycopg.connect(POSTGRES_CONNECTION) as conn:
        cur: psycopg.Cursor
        with conn.cursor() as cur:
            copy: psycopg.Copy
            with cur.copy("COPY Customers (FirstName, LastName, Email, Phone, Address, City,"
                            " State, ZipCode, Country, LoyaltyPoints) FROM STDIN") as copy:
                for customer in customers:
                    copy.write_row(customer)


if __name__ == '__main__':
    fake_gen = Faker('en_US')
    fake_gen.add_provider(geo_area_provider)
    fake_gen.add_provider(store_type_provider)

    create_suppliers(fake_gen)
    create_manufacturers(fake_gen)
    create_supplier_manufacturer_conn()
    create_products(fake_gen)
    create_inventory(fake_gen)
    create_distributors(fake_gen)
    create_retailers(fake_gen)
    create_customers(fake_gen)
    create_kafka_topics()
    list_kafka_topics() #probably not needed
    
