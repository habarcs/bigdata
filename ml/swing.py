# Simple program that creates a Swing instance and gives recommendations for items.
# See: # Simple program that creates a Swing instance and gives recommendations for items.


from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.connectors import KafkaSource
from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment
from pyflink.ml.recommendation.swing import Swing


# Creates a new StreamExecutionEnvironment
env = StreamExecutionEnvironment.get_execution_environment()

# Creates a StreamTableEnvironment
t_env = StreamTableEnvironment.create(env)

  
t_env.get_config().set(
    "pipeline.jars",
    "file:///opt/flink-sql-connector-kafka.jar;file:///opt/flink-sql-connector-postgresql.jar")

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

t_env.create_temporary_table(
    "kafka_orders",
    TableDescriptor.for_connector("kafka")
    .schema(kafka_schema)
    .option("topic", "Orders")
    .option("properties.bootstrap.servers", "kafka:9092")
    .option("format", "json")
    .option("properties.group.id", "flinventory")
    .build())   
    
kafka_source =t_env.from_path("kafka_orders")
    
    # Add Kafka source to the environment
data_stream = env.from_source(
    kafka_source,
    watermark_strategy=None,  # Add a watermark strategy if needed
    source_name="KafkaSource"
)
    
# Parse Kafka messages into the desired format
# Assuming messages are in the format "user_id,item_id"
parsed_stream = data_stream.map(
    lambda x: tuple(map(int, x.split(","))),  # Parses "user,item" into (user, item)
    output_type=Types.TUPLE([Types.LONG(), Types.LONG()])
)  

# Convert the parsed stream to a table
input_table = t_env.from_data_stream(
    parsed_stream,
    schema=["user", "item"]
)


# Creates a swing object and initialize its parameters.
swing = Swing()
    .set_item_col('item')
    .set_user_col("user")
    .set_min_user_behavior(1)

# Transforms the data to Swing algorithm result.
output_table = swing.transform(input_table)

# Extracts and display the results.
field_names = output_table[0].get_schema().get_field_names()

results = t_env.to_data_stream(
    output_table[0]).execute_and_collect()

for result in results:
    main_item = result[field_names.index(swing.get_item_col())]
    item_rank_score = result[1]
    print(f'item: {main_item}, top-k similar items: {item_rank_score}')
