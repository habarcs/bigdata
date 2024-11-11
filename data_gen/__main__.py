from faker import Faker

from data_gen import geo_area_provider, store_type_provider
from data_gen.create_initial_data import create_suppliers, create_manufacturers, create_supplier_manufacturer_conn, \
    create_products, create_inventory, create_distributors, create_retailers, create_customers
from data_gen.kafka_producer import create_kafka_topics, event_generation_loop


def main():
    fake = Faker('en_US')
    fake.add_provider(geo_area_provider)
    fake.add_provider(store_type_provider)

    create_suppliers(fake)
    create_manufacturers(fake)
    create_supplier_manufacturer_conn()
    create_products(fake)
    create_inventory(fake)
    create_distributors(fake)
    create_retailers(fake)
    create_customers(fake)

    create_kafka_topics()
    event_generation_loop(fake)


if __name__ == '__main__':
    main()
