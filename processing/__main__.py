from pyflink.table import EnvironmentSettings, TableEnvironment, TableDescriptor, Schema, DataTypes
from pyflink.table.expressions import col, call


def main():
    # Create a streaming TableEnvironment
    env_settings = EnvironmentSettings.in_streaming_mode()
    table_env = TableEnvironment.create(env_settings)
    table_env.get_config().set("python.execution-mode", "thread")
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
        .column("product_id", DataTypes.BIGINT().not_null()) \
        .column("retailer_id", DataTypes.BIGINT().not_null()) \
        .column("quantity_on_hand", DataTypes.INT()) \
        .column("reorder_level", DataTypes.INT()) \
        .primary_key("product_id", "retailer_id") \
        .build()

    postgres_daily_aggregates_schema = Schema.new_builder() \
        .column("ds", DataTypes.STRING().not_null()) \
        .column("product_id", DataTypes.BIGINT().not_null()) \
        .column("retailer_id", DataTypes.BIGINT().not_null()) \
        .column("order_status", DataTypes.STRING().not_null()) \
        .column("total_item_quantity", DataTypes.INT()) \
        .column("avg_real_shipping_days", DataTypes.DOUBLE()) \
        .column("avg_scheduled_shipping_days", DataTypes.DOUBLE()) \
        .column("avg_late_risk", DataTypes.DOUBLE()) \
        .primary_key("product_id", "retailer_id", "ds", "order_status") \
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
        .option("scan.startup.mode", "group-offsets")
        .option('properties.auto.offset.reset', 'earliest')
        .build()
    )

    # Create Postgres sink table
    table_env.create_temporary_table(
        "postgres_inventory",
        TableDescriptor.for_connector("jdbc")
        .schema(postgres_inventory_schema)
        .option("url", "jdbc:postgresql://sql-database:5432/postgres")
        .option("table-name", 'inventory')
        .option("username", "postgres")
        .option("password", "supersecret")
        .option("sink.buffer-flush.max-rows", "1")
        .build()
    )

    # Create postgres table storing daily demand
    table_env.create_temporary_table(
        "postgres_daily_aggregates",
        TableDescriptor.for_connector("jdbc")
        .schema(postgres_daily_aggregates_schema)
        .option("url", "jdbc:postgresql://sql-database:5432/postgres")
        .option("table-name", 'daily_aggregates')
        .option("username", "postgres")
        .option("password", "supersecret")
        .option("sink.buffer-flush.max-rows", "1")
        .build()
    )

    orders = table_env.from_path("kafka_orders").select(
        col('product_id').alias("order_product_id"),
        col('retailer_id').alias("order_retailer_id"),
        col('item_quantity'),
        col('status'),
        col("real_shipping_days"),
        col("scheduled_shipping_days"),
        col("late_risk"),
        call("SUBSTRING", col('order_date'), 0, 10).alias("order_date")
    )
    inventory = table_env.from_path("postgres_inventory").select(
        col('product_id'),
        col('retailer_id'),
        col('quantity_on_hand'),
        col('reorder_level')
    )

    updated_inventory = (
        orders.left_outer_join(inventory)
        .where(
            (col('order_product_id') == col("product_id"))
            & (col('order_retailer_id') == col("retailer_id"))
        )
        .select(
            col('product_id'),
            col('retailer_id'),
            call("GREATEST", col('quantity_on_hand') - col('item_quantity'), 0).alias("quantity_on_hand"),
            col('reorder_level')
        )
    )

    daily_aggregates = (
        orders
        .group_by(col("order_product_id"), col("order_retailer_id"), col("order_date"), col("status"))
        .select(
            col("order_date").alias("ds"),
            col("order_product_id").alias("product_id"),
            col("order_retailer_id").alias("retailer_id"),
            col("status").alias("order_status"),
            call("SUM", col("item_quantity")).alias("total_item_quantity"),
            call("AVG", col("real_shipping_days")).alias("avg_real_shipping_days"),
            call("AVG", col("scheduled_shipping_days")).alias("avg_scheduled_shipping_days"),
            call("AVG", col("late_risk")).alias("avg_late_risk")
        )
    )

    daily_aggregates.execute_insert("postgres_daily_aggregates")
    updated_inventory.execute_insert("postgres_inventory")


if __name__ == '__main__':
    main()
