from faker import Faker
import psycopg
import os
import random

# TODO refer to the postgres host by sql-database once this code runs in docker
# TODO less important: secret should not be clear
POSTGRES_CONNECTION = "dbname=postgres user=postgres host=localhost port=5432 password=supersecret"

NUM_SUPPLIERS = int(os.getenv("NUM_SUPPLIERS", 200))
NUM_MANUFACTURERS = int(os.getenv("NUM_MANUFACTURERS", 30))


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
    conn: psycopg.Connection
    with psycopg.connect(POSTGRES_CONNECTION) as conn:
        cur: psycopg.Cursor
        with conn.cursor() as cur:
            supplier_ids = [row[0] for row in cur.execute("SELECT SupplierID from Suppliers").fetchall()]
            manufacturer_ids = [row[0] for row in cur.execute("SELECT ManufacturerID from Manufacturers").fetchall()]
            print(supplier_ids)
            print(manufacturer_ids)
            # TODO finish multi-multi conn


if __name__ == '__main__':
    fake_gen = Faker()
    create_suppliers(fake_gen)
    create_manufacturers(fake_gen)
    create_supplier_manufacturer_conn()
