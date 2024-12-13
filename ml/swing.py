from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.connectors import KafkaSource
from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment, TimeCharacteristic
from pyflink.table import StreamTableEnvironment
from pyflink.table.descriptors import Schema, TableDescriptor, DataTypes
from pyflink.ml.recommendation.swing import Swing


# Create Flink Execution and Table Environments
env = StreamExecutionEnvironment.get_execution_environment()
env.set_stream_time_characteristic(TimeCharacteristic.EventTime)
t_env = StreamTableEnvironment.create(env)

# Configure External JARs
t_env.get_config().set(
    "pipeline.jars",
    "file:///opt/flink-sql-connector-kafka.jar;file:///opt/flink-sql-connector-postgresql.jar"
)

# Define Kafka Table Schema
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
    .column_by_expression("event_time", "CAST(OrderDate AS TIMESTAMP(3))") \
    .watermark("event_time", "event_time - INTERVAL '5' SECOND") \
    .build()

# Register the Kafka Table
t_env.create_temporary_table(
    "kafka_orders",
    TableDescriptor.for_connector("kafka")
    .schema(kafka_schema)
    .option("topic", "Orders")
    .option("properties.bootstrap.servers", "kafka:9092")
    .option("format", "json")
    .option("properties.group.id", "flinventory")
    .build()
)

# Read data directly from the Kafka table
kafka_table = t_env.from_path("kafka_orders")

# Extract "ProductID" and "CustomerID" columns into a data stream
product_customer_stream = t_env.to_data_stream(
    kafka_table.select("CustomerID, ProductID")
)

# Convert to the required format for Swing
# Assumes CustomerID corresponds to the "user" column and ProductID to the "item" column
parsed_stream = product_customer_stream.map(
    lambda row: (row[0], row[1]),  # (CustomerID, ProductID)
    output_type=Types.TUPLE([Types.INT(), Types.INT()])
)

# Convert the parsed stream to a Flink Table
input_table = t_env.from_data_stream(
    parsed_stream,
    schema=["user", "item"]
)

# Configure Swing Recommendation
swing = Swing() \
    .set_item_col('item') \
    .set_user_col("user") \
    .set_min_user_behavior(1)

# Apply the Swing Algorithm
output_table = swing.transform(input_table)

# Convert the result table back to a data stream and print the output
results = t_env.to_data_stream(output_table[0])

field_names = output_table[0].get_schema().get_field_names()

for result in results.execute_and_collect():
    main_item = result[field_names.index(swing.get_item_col())]
    item_rank_score = result[1]
    print(f'Item: {main_item}, Top-K Similar Items: {item_rank_score}')

# Execute the Flink Job
env.execute("Swing Recommendation Job")
