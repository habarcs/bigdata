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
        .column("transaction_type", DataTypes.STRING()) \
        .column("real_shipping_days", DataTypes.INT()) \
        .column("scheduled_shipping_days", DataTypes.INT()) \
        .column("delivery_status", DataTypes.STRING()) \
        .column("late_risk", DataTypes.INT()) \
        .column("order_date", DataTypes.STRING()) \
        .column("order_id", DataTypes.INT()) \
        .column("product_id", DataTypes.INT()) \
        .column("item_quantity", DataTypes.INT()) \
        .column("status", DataTypes.STRING()) \
        .column("shipping_data", DataTypes.STRING()) \
        .column("shipping_mode", DataTypes.STRING()) \
        .column("customer_id", DataTypes.INT()) \
        .column("retailer_id", DataTypes.INT()) \
        .build()

    postgres_inventory_schema = Schema.new_builder() \
        .column("product_id", DataTypes.BIGINT()) \
        .column("retailer_id", DataTypes.BIGINT()) \
        .column("quantity_on_hand", DataTypes.INT()) \
        .column("reorder_level", DataTypes.INT()) \
        .build()

    # Create Kafka source table
    table_env.create_temporary_table(
        "kafka_orders",
        TableDescriptor.for_connector("kafka")
        .schema(kafka_schema)
        .option("topic", "orders")
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
        .option("table-name", 'inventory')
        .option("username", "postgres")
        .option("password", "supersecret")
        .build())

    orders = table_env.from_path("kafka_orders").select(
        col('product_id'),
        col('retailer_id'),
        col('item_quantity'),
        col('order_id')
    )
    inventory = table_env.from_path("postgres_inventory").select(
        col('product_id').alias("inventory_product_id"),
        col('retailer_id').alias("inventory_retailer_id"),
        col('quantity_on_hand')
    )
    result = (orders.left_outer_join(inventory)
              .where((col('product_id') == col("inventory_product_id"))
                     & (col('retailer_id') == col("inventory_retailer_id"))))

    result.execute()


if __name__ == '__main__':
    main()
