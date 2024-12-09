from pyflink.table import EnvironmentSettings, TableEnvironment, TableDescriptor, Schema, DataTypes


def main():
    # Create a streaming TableEnvironment
    env_settings = EnvironmentSettings.in_streaming_mode()
    table_env = TableEnvironment.create(env_settings)
    table_env.get_config().set(
        "pipeline.jars",
        "file:///opt/postgresql.jar;file:///opt/flink-connector-kafka.jar")

    # Define Kafka table schema
    kafka_schema = Schema.new_builder() \
        .column("TransactionType", DataTypes.STRING()) \
        .column("RealShippingDays", DataTypes.INT()) \
        .column("ScheduledShippingDays", DataTypes.INT()) \
        .column("DeliveryStatus", DataTypes.STRING()) \
        .column("LateRisk", DataTypes.INT()) \
        .column("OrderDate", DataTypes.STRING()) \
        .column("OrderID", DataTypes.INT()) \
        .column("ProductID", DataTypes.INT()) \
        .column("ItemQuantity", DataTypes.INT()) \
        .column("Status", DataTypes.STRING()) \
        .column("ShippingData", DataTypes.STRING()) \
        .column("ShippingMode", DataTypes.STRING()) \
        .column("CustomerID", DataTypes.INT()) \
        .column("RetailerID", DataTypes.INT()) \
        .build()

    postgres_inventory_schema = Schema.new_builder() \
        .column("ProductID", DataTypes.BIGINT()) \
        .column("RetailerID", DataTypes.BIGINT()) \
        .column("QuantityOnHand", DataTypes.INT()) \
        .column("ReorderLevel", DataTypes.INT()) \
        .build()

    # Create Kafka source table
    table_env.create_temporary_table(
        "kafka_orders",
        TableDescriptor.for_connector("kafka")
        .schema(kafka_schema)
        .option("topic", "Orders")
        .option("properties.bootstrap.servers", "kafka:9092")
        .option("format", "json")
        .build())

    # Create Postgres sink table
    table_env.create_temporary_table(
        "postgres_inventory",
        TableDescriptor.for_connector("jdbc")
        .schema(postgres_inventory_schema)
        .option("url", "jdbc:postgresql://sql-database:5432/postgres")
        .option("table-name", "Inventory")
        .option("username", "postgres")
        .option("password", "supersecret")
        .build())


if __name__ == '__main__':
    main()
