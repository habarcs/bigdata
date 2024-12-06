# from pyflink.table import EnvironmentSettings, TableEnvironment, TableDescriptor, Schema, DataTypes

#def main():
    
#     # create a streaming TableEnvironment
#     env_settings = EnvironmentSettings.in_streaming_mode()
#     table_env = TableEnvironment.create(env_settings)

#     table_env.create_temporary_table(
#         'kafka_orders',
#         TableDescriptor.for_connector('kafka')
#         .schema(Schema.new_builder()
#                 .column("TransactionType", DataTypes.STRING ) # "PAYMENT"
#                 .column("RealShippingDays", DataTypes.INT) # 2
#                 .column("ScheduledShippingDays", DataTypes.INT) # 2 
#                 .column("DeliveryStatus", DataTypes.STRING) # "Shipping on time"
#                 .column("LateRisk", DataTypes.INT) # 0
#                 .column("OrderData", DataTypes.TIMESTAMP) # "10/4/2016 22:55"
#                 .column("OrderID", DataTypes.INT) # 44046
#                 .column("ProductID", DataTypes.INT) # 191
#                 .column("ItemQuantity", DataTypes.INT) # 3
#                 .column("Status", DataTypes.STRING) # "PENDING_PAYMENT"
#                 .column("ShippingData", DataTypes.TIMESTAMP) # "10/6/2016 22:55"
#                 .column("ShippingMode", DataTypes.STRING) # "Second Class"
#                 .column("CustomerID", DataTypes.INT) # 5197
#                 .column("RetailerID", DataTypes.INT) # 1
#                 .build())
#         .build())

#     table = table_env.from_path("kafka_orders")
#     table.execute().print()

# # Define Kafka connector properties
#     kafka_connector = (
#         TableDescriptor.for_connector("kafka")
#         .option("topic", "Orders")  # Your Kafka topic name
#         .option("properties.bootstrap.servers", "kafka:9092")  # Kafka broker address
#         .option("properties.group.id", "flink_group")  # Consumer group ID
#         .option("scan.startup.mode", "earliest-offset")  # Start reading from earliest offset
#         .format(FormatDescriptor.for_format("json")
#                 .option("timestamp-format.standard", "ISO-8601")  # Correct without the 'json.' prefix
#                 .build())  # JSON format with ISO-8601 timestamps
#         .schema(kafka_schema)  # Use the schema built above
#         .build()
#     )






# # 3. create sink Table
#     table_env.execute_sql("""
#         CREATE TABLE print (
#             id INT,
#             data STRING
#         ) WITH (
#             'connector' = 'print'
#         )   
#     """)

from pyflink.table import EnvironmentSettings, TableEnvironment, TableDescriptor, Schema, DataTypes

def main():
    
# Create a streaming TableEnvironment
    env_settings = EnvironmentSettings.in_streaming_mode()
    table_env = TableEnvironment.create(env_settings)

    # Define Kafka table schema
    kafka_schema = Schema.new_builder()
    .column("TransactionType", DataTypes.STRING()) 
    .column("RealShippingDays", DataTypes.INT()) 
    .column("ScheduledShippingDays", DataTypes.INT()) 
    .column("DeliveryStatus", DataTypes.STRING()) 
    .column("LateRisk", DataTypes.INT()) 
    .column("OrderData", DataTypes.TIMESTAMP()) 
    .column("OrderID", DataTypes.INT()) 
    .column("ProductID", DataTypes.INT()) 
    .column("ItemQuantity", DataTypes.INT())
    .column("Status", DataTypes.STRING()) 
    .column("ShippingData", DataTypes.TIMESTAMP()) 
    .column("ShippingMode", DataTypes.STRING()) 
    .column("CustomerID", DataTypes.INT()) 
    .column("RetailerID", DataTypes.INT()) 
    .build()

    # Create Kafka source table
    table_env.create_temporary_table(
    'kafka_orders',
    TableDescriptor.for_connector('kafka')
    .schema(kafka_schema)
    .option("topic", "Orders")  # Kafka topic
    .option("properties.bootstrap.servers", "kafka:9092")  # Kafka broker
    .option("properties.group.id", "flink_group")  # Consumer group
    .option("scan.startup.mode", "earliest-offset")  # Start reading from earliest offset
    .format(
        FormatDescriptor.for_format("json")
        .option("timestamp-format.standard", "ISO-8601")  # Timestamp format
        .build()
    )
    .build()
    )

    # Define a sink table (for testing, using print connector) I guess this will be the transactions table?
    table_env.execute_sql("""
    CREATE TABLE print_sink (
        OrderID INT,
        ProductID INT,
        ItemQuantity INT, 
        RetailerID INT 
    ) WITH (
        'connector' = 'print'  
    )
    """)

    # Query Kafka table and write to sink
    table_env.execute_sql("""
    INSERT INTO print_sink
    SELECT *
    FROM kafka_orders
    """)







if __name__ == '__main__':
    main()
