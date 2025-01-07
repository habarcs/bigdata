from confluent_kafka.admin import AdminClient, ClusterMetadata
from data_gen import KAFKA_CONF

if __name__ == '__main__':
    admin = AdminClient(KAFKA_CONF)
    topics: ClusterMetadata = admin.list_topics()
    if "orders" in topics.topics:
        exit(0)
    exit(1)
