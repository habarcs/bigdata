CREATE TABLE Suppliers (
    SupplierID STRING PRIMARY KEY,
    SupplierName STRING,
    ContactName STRING,
    ContactEmail STRING,
    ContactPhone STRING,
    Address STRING,
    City STRING,
    State STRING,
    ZipCode STRING,
    Country STRING,
    Rating INT
) WITH (
    KAFKA_TOPIC='suppliers',
    VALUE_FORMAT='JSON',
    KEY='SupplierID'
);

CREATE TABLE Manufacturers (
    ManufacturerID STRING PRIMARY KEY,
    ManufacturerName STRING,
    ContactName STRING,
    ContactEmail STRING,
    ContactPhone STRING,
    Address STRING,
    City STRING,
    State STRING,
    ZipCode STRING,
    Country STRING,
    ProductionCapacity INT
) WITH (
    KAFKA_TOPIC='manufacturers',
    VALUE_FORMAT='JSON',
    KEY='ManufacturerID'
);

CREATE TABLE Products (
    ProductID STRING PRIMARY KEY,
    ProductName STRING,
    Category STRING,
    Description STRING,
    UnitPrice DECIMAL(10, 2),
    ManufacturerID STRING,
    SupplierID STRING,
    WarrantyPeriod STRING
) WITH (
    KAFKA_TOPIC='products',
    VALUE_FORMAT='JSON',
    KEY='ProductID'
);

CREATE TABLE Distributors (
    DistributorID STRING PRIMARY KEY,
    DistributorName STRING,
    ContactName STRING,
    ContactEmail STRING,
    ContactPhone STRING,
    Address STRING,
    City STRING,
    State STRING,
    ZipCode STRING,
    Country STRING,
    DistributionArea STRING
) WITH (
    KAFKA_TOPIC='distributors',
    VALUE_FORMAT='JSON',
    KEY='DistributorID'
);

CREATE TABLE Retailers (
    RetailerID STRING PRIMARY KEY,
    RetailerName STRING,
    ContactName STRING,
    ContactEmail STRING,
    ContactPhone STRING,
    Address STRING,
    City STRING,
    State STRING,
    ZipCode STRING,
    Country STRING,
    StoreType STRING
) WITH (
    KAFKA_TOPIC='retailers',
    VALUE_FORMAT='JSON',
    KEY='RetailerID'
);

CREATE TABLE Customers (
    CustomerID STRING PRIMARY KEY,
    FirstName STRING,
    LastName STRING,
    Email STRING,
    Phone STRING,
    Address STRING,
    City STRING,
    State STRING,
    ZipCode STRING,
    Country STRING,
    LoyaltyPoints INT
) WITH (
    KAFKA_TOPIC='customers',
    VALUE_FORMAT='JSON',
    KEY='CustomerID'
);


-- streams 

CREATE STREAM OrdersStream (
    OrderID STRING,
    CustomerID STRING,
    OrderDate BIGINT,  -- Use UNIX epoch time in milliseconds for the timestamp
    TotalAmount DECIMAL(10, 2),
    OrderStatus STRING,
    ShippingID STRING,
    RetailerID STRING
) WITH (
    KAFKA_TOPIC='orders_status',
    VALUE_FORMAT='JSON'
);


CREATE STREAM InventoryStream (
    InventoryID STRING,
    ProductID STRING,
    QuantityOnHand INT,
    ReorderLevel INT,
    Location STRING
) WITH (
    KAFKA_TOPIC='inventory_status',
    VALUE_FORMAT='JSON'
);

CREATE STREAM ShippingStream (
    ShippingID STRING,
    OrderID STRING,
    DistributorID STRING,
    ShippingDate BIGINT,  -- Use UNIX epoch time in milliseconds
    ShippingMethod STRING,
    ShippingStatus STRING,
    DeliveryStatus STRING
) WITH (
    KAFKA_TOPIC='shipping_status',
    VALUE_FORMAT='JSON'
);

CREATE STREAM PaymentsStream (
    PaymentID STRING,
    OrderID STRING,
    PaymentDate BIGINT,  -- Use UNIX epoch time in milliseconds
    Amount DECIMAL(10, 2),
    PaymentMethod STRING,  
    PaymentStatus STRING
) WITH (
    KAFKA_TOPIC='payments_status',
    VALUE_FORMAT='JSON'
);


