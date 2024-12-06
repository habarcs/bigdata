from pyflink.table import EnvironmentSettings, TableEnvironment, TableDescriptor, Schema, DataTypes

def main():
    # create a streaming TableEnvironment
    env_settings = EnvironmentSettings.in_streaming_mode()
    table_env = TableEnvironment.create(env_settings)

    table_env.create_temporary_table(
        'kafka_orders',
        TableDescriptor.for_connector('datagen')
        .schema(Schema.new_builder()
                .column("TransactionType", DataTypes.STRING ) # "PAYMENT"
                .column("RealShippingDays", DataTypes.INT) # 2
                .column("ScheduledShippingDays", DataTypes.INT) # 2 
                .column("DeliveryStatus", DataTypes.STRING) # "Shipping on time"
                .column("LateRisk", DataTypes.INT) # 0
                .column("OrderData", DataTypes.TIMESTAMP) # "10/4/2016 22:55"
                .column("OrderID", DataTypes.INT) # 44046
                .column("ProductID", DataTypes.INT) # 191
                .column("ItemQuantity", DataTypes.INT) # 3
                .column("Status", DataTypes.STRING) # "PENDING_PAYMENT"
                .column("ShippingData", DataTypes.TIMESTAMP) # "10/6/2016 22:55"
                .column("ShippingMode", DataTypes.STRING) # "Second Class"
                .column("CustomerID", DataTypes.INT) # 5197
                .column("RetailerID", DataTypes.INT) # 1
                .build())
        .build())

    table = table_env.from_path("random_source")
    table.execute().print()

if __name__ == '__main__':
    main()
