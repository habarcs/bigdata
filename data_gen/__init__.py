import socket

from faker.providers import DynamicProvider

# TODO less important: secret should not be clear
POSTGRES_CONNECTION = "postgresql+psycopg://postgres:supersecret@sql-database:5432/postgres"
KAFKA_CONF = {'bootstrap.servers': 'kafka:9092',
              'client.id': socket.gethostname()}
