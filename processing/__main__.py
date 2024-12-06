from pyflink.table import EnvironmentSettings, TableEnvironment, TableDescriptor, Schema, DataTypes

def main():
    # create a streaming TableEnvironment
    env_settings = EnvironmentSettings.in_streaming_mode()
    table_env = TableEnvironment.create(env_settings)

    table_env.create_temporary_table(
        'kafka_orders',
        TableDescriptor.for_connector('datagen')
        .schema(Schema.new_builder()
                .column("TransactionType", "PAYMENT")
                .column("RealShippingDays", 2)
                .column("ScheduledShippingDays", 2)
                .column("DeliveryStatus", "Shipping on time")
                .column("LateRisk", 0)
                .column("OrderData", "10/4/2016 22:55")
                .column("OrderID", 44046)
                .column("ProductID", 191)
                .column("ItemQuantity", 3)
                .column("Status", "PENDING_PAYMENT")
                .column("ShippingData", "10/6/2016 22:55")
                .column("ShippingMode", "Second Class")
                .column("CustomerID", 5197)
                .column("RetailerID", 1)
                .build())
        .build())

    table = table_env.from_path("random_source")
    table.execute().print()

if __name__ == '__main__':
    main()
