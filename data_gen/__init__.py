import socket

# TODO less important: secret should not be clear
# Use the local version if you are not running the script in a docker container but your own machine
POSTGRES_CONNECTION = "postgresql+psycopg://postgres:supersecret@sql-database:5432/postgres"
POSTGRES_CONNECTION_LOCAL = "postgresql+psycopg://postgres:supersecret@localhost:5432/postgres"
KAFKA_CONF = {'bootstrap.servers': 'kafka:9092',
              'client.id': socket.gethostname()}
KAFKA_CONF_LOCAL = {'bootstrap.servers': 'localhost:9092',
                    'client.id': socket.gethostname()}
