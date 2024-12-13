from pyflink.table import EnvironmentSettings, TableEnvironment, TableDescriptor, Schema, DataTypes
from pyflink.table.expressions import col


def main():
    # Create a streaming TableEnvironment
    env_settings = EnvironmentSettings.in_streaming_mode()
    table_env = TableEnvironment.create(env_settings)
    table_env.get_config().set(
        "pipeline.jars",
        "file:///opt/flink-sql-connector-kafka.jar;file:///opt/flink-connector-jdbc.jar;file:///opt/postgresql.jar")

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
        .option("properties.group.id", "flinventory")
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

    orders = table_env.from_path("kafka_orders").select(
        col("ProductID"),
        col("RetailerID"),
        col("ItemQuantity"),
        col("OrderID")
    )
    inventory = table_env.from_path("postgres_inventory").select(
        col("ProductID").alias("inventory_product_id"),
        col("RetailerID").alias("inventory_retailer_id"),
        col("QuantityOnHand")
    )
    result = (orders.left_outer_join(inventory)
              .where((col("ProductID") == col("inventory_product_id"))
                     & (col("RetailerID") == col("inventory_retailer_id"))))

    result.execute()


if __name__ == '__main__':
    main()
