CREATE TABLE Suppliers
(
    SupplierID   bigint PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    SupplierName text    NOT NULL,
    ContactName  text    NOT NULL,
    ContactEmail text    NOT NULL,
    ContactPhone text    NOT NULL,
    Address      text    NOT NULL,
    City         text    NOT NULL,
    State        text    NOT NULL,
    ZipCode      text    NOT NULL,
    Country      text    NOT NULL,
    Rating       integer NOT NULL
);

CREATE TABLE Manufacturers
(
    ManufacturerID     bigint PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    ManufacturerName   text    NOT NULL,
    ContactName        text    NOT NULL,
    ContactEmail       text    NOT NULL,
    ContactPhone       text    NOT NULL,
    Address            text    NOT NULL,
    City               text    NOT NULL,
    State              text    NOT NULL,
    ZipCode            text    NOT NULL,
    Country            text    NOT NULL,
    ProductionCapacity integer NOT NULL
);

CREATE TABLE Supplier_Manufacturer
(
    SupplierID     bigint REFERENCES Suppliers ON DELETE CASCADE,
    ManufacturerID bigint REFERENCES Manufacturers ON DELETE CASCADE,
    PRIMARY KEY (SupplierID, ManufacturerID)
)

CREATE TABLE Products
(
    ProductID      bigint PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    ProductName    text           NOT NULL,
    Category       text           NOT NULL,
    Description    text           NOT NULL,
    UnitPrice      DECIMAL(10, 2) NOT NULL,
    ManufacturerID bigint REFERENCES Manufacturers,
    WarrantyPeriod text           NOT NULL
);

CREATE TABLE Inventory
(
    InventoryID    bigint PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    ProductID      bigint  REFERENCES Products UNIQUE,
    QuantityOnHand integer NOT NULL,
    ReorderLevel   integer NOT NULL,
    Location       text    NOT NULL
);

CREATE TABLE Distributors
(
    DistributorID    bigint PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    DistributorName  text NOT NULL,
    ContactName      text NOT NULL,
    ContactEmail     text NOT NULL,
    ContactPhone     text NOT NULL,
    Address          text NOT NULL,
    City             text NOT NULL,
    State            text NOT NULL,
    ZipCode          text NOT NULL,
    Country          text NOT NULL,
    DistributionArea text NOT NULL
);

CREATE TABLE Retailers
(
    RetailerID   bigint PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    RetailerName text NOT NULL,
    ContactName  text NOT NULL,
    ContactEmail text NOT NULL,
    ContactPhone text NOT NULL,
    Address      text NOT NULL,
    City         text NOT NULL,
    State        text NOT NULL,
    ZipCode      text NOT NULL,
    Country      text NOT NULL,
    StoreType    text NOT NULL
);

CREATE TABLE Customers
(
    CustomerID    bigint PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    FirstName     text    NOT NULL,
    LastName      text    NOT NULL,
    Email         text    NOT NULL,
    Phone         text    NOT NULL,
    Address       text    NOT NULL,
    City          text    NOT NULL,
    State         text    NOT NULL,
    ZipCode       text    NOT NULL,
    Country       text    NOT NULL,
    LoyaltyPoints integer NOT NULL
);
