CREATE TABLE Suppliers (
    SupplierID STRING PRIMARY KEY,
    SupplierName STRING,
    City STRING,
    State STRING,
    ZipCode STRING,
    Country STRING,
    Rating INT
) WITH (KAFKA_TOPIC = 'suppliers', VALUE_FORMAT = 'JSON');


CREATE TABLE Manufacturers (
    ManufacturerID STRING PRIMARY KEY,
    ManufacturerName STRING,
    City STRING,
    State STRING,
    ZipCode STRING,
    Country STRING,
    ProductionCapacity INT
    
) WITH (
    KAFKA_TOPIC = 'manufacturers',
    VALUE_FORMAT = 'JSON'
);



-- New: DONE

CREATE TABLE Supplier_Manufacturer_Contract (
  Supply_Contract_ID STRING PRIMARY KEY,
  Supplier_ID STRING,
  Manufacturer_ID STRING,
  Contract_Start_Date STRING,
  Contract_End_Date STRING
)WITH (
    KAFKA_TOPIC = 'supplier_manufacturer_contract',
    VALUE_FORMAT = 'JSON'
);

-- New: DONE
CREATE STREAM Supplied_Material (
  Supplied_Material_ID STRING KEY,
  Supply_Contract_ID STRING,
  Material_Name STRING,
  Material_Price DECIMAL(10, 2),
  Material_Quantity INT,
  Requested_Date STRING,
  Delivery_Date STRING,
  Delivery_Status STRING
)WITH (
    KAFKA_TOPIC = 'supplied_material_status',
    VALUE_FORMAT = 'JSON'
);

-- DONE
CREATE TABLE Products (
    ProductID STRING PRIMARY KEY,
    ProductName STRING,  --Product Name
    ProductDepartment STRING,  -- Department Name
    ProductCategory STRING,  -- Product Category Name
    UnitPrice DECIMAL(10, 2),  --Product Price
    ManufacturerID STRING FOREIGN KEY,  -- Manufacturer ID ::New
    WarrantyPeriod STRING,
    ProductStatus STRING  -- TODO: Product stock status
) WITH (
    KAFKA_TOPIC = 'products',
    VALUE_FORMAT = 'JSON',
    KEY = 'ProductID'
);


-- DONE
CREATE TABLE Distributors (
    DistributorID STRING PRIMARY KEY,
    DistributorName STRING,
    City STRING,
    State STRING,
    ZipCode STRING,
    Country STRING,
    DistributionArea STRING
) WITH (
    KAFKA_TOPIC = 'distributors',
    VALUE_FORMAT = 'JSON'
);

CREATE TABLE Retailers (
    RetailerID STRING PRIMARY KEY,
    RetailerName STRING,
    Latitude DECIMAL(10, 3),  --Latitude
    Longitude DECIMAL(10, 3),  --Longitude
    City STRING,   -- Customer City - where the order is made
    State STRING,
    ZipCode STRING,
    Country STRING
) WITH (
    KAFKA_TOPIC = 'retailers',
    VALUE_FORMAT = 'JSON'
);
CREATE TABLE Customers (
    CustomerID STRING PRIMARY KEY, -- unique user id
    FirstName STRING,  -- New: Customer's first name
    LastName STRING,  -- New: Customer's last name
    City STRING,  -- New: Customer's city
    State STRING,  -- New: Customer's state
    ZipCode STRING,  -- New: Customer's zipcode
    Country STRING,  -- New: Customer's country
    LoyaltyPoints INT,
    CustomerSegment STRING -- New: Customer segment (e.g., Consumer, Corporate)
) WITH (
    KAFKA_TOPIC = 'customers',
    VALUE_FORMAT = 'JSON'
);


-- streams 
CREATE STREAM OrdersStream (
    OrderID STRING KEY, -- Order Item Id
    CustomerID STRING,  -- Order Customer Id
    OrderDate STRING,  --Order Date
    TotalAmount DECIMAL(10, 2),  --Sales Per Customer
    OrderStatus STRING,  -- Order Status
    PaymentMethod STRING,  -- Type
    ShippingID STRING,
    RetailerID STRING,
    OrderItemDiscount DECIMAL(10, 2),  --Total item discount value
    OrderItemDiscountRate DECIMAL(10, 2),  --Total item discount rate
    OrderItemTotal DECIMAL(10, 2),  -- New: Total amount of the order after discount
    OrderProfitPerOrder DECIMAL(10, 2),  -- New: Order Profit Per Order
    OrderQuantity INT,  -- Order Item Quantity
    OrderCity STRING,  -- New: City where the order is placed
    OrderState STRING,  -- New: State where the order is placed
    OrderCountry STRING,  -- New: Country where the order is placed
    OrderRegion STRING -- New: Region where the order is placed
) WITH (
    KAFKA_TOPIC = 'order_info',
    VALUE_FORMAT = 'JSON'
);


CREATE STREAM InventoryStream (
    InventoryID STRING,
    ProductID STRING,
    QuantityOnHand INT,
    ReorderLevel INT,
    Location STRING
) WITH (
    KAFKA_TOPIC = 'inventory_status',
    VALUE_FORMAT = 'JSON'
);
CREATE STREAM ShippingStream (
    ShippingID STRING,
    OrderID STRING,
    DistributorID STRING,
    ShippingDate BIGINT,  -- shipping date (DateOrders)
    ShippingMethod STRING,  -- New: Shipping mode (e.g., Standard, First Class)
    LateDeliveryRisk INT,  -- Late_delivery_risk
    DeliveryStatus STRING,  -- New: Status of delivery (Advance, Late, etc.)
    DaysForShippingReal INT,  -- New: Actual shipping days
    DaysForShippingScheduled INT -- New: Scheduled shipping days
) WITH (
    KAFKA_TOPIC = 'shipping_status',
    VALUE_FORMAT = 'JSON'
);
-- CREATE STREAM PaymentsStream (
--     PaymentID STRING,
--     OrderID STRING, 
--     PaymentDate BIGINT, 
--     Amount DECIMAL(10, 2),
--     PaymentMethod STRING, -- Type  
--     PaymentStatus STRING, 
-- ) WITH (
--     KAFKA_TOPIC='payments_status',
--     VALUE_FORMAT='JSON'
-- );