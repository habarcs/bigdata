CREATE TABLE "Products"
(
    "ProductID"    bigint PRIMARY KEY,
    "ProductName"  text           NOT NULL,
    "Category"     text           NOT NULL,
    "ProductPrice" DECIMAL(10, 2) NOT NULL
);

CREATE TABLE "Retailers"
(
    "RetailerID"      bigint PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    "RetailerName"    text NOT NULL,
    "RetailerCountry" text NOT NULL,
    "RetailerState"   text NOT NULL,
    "RetailerCity"    text NOT NULL,
    UNIQUE ("RetailerCountry", "RetailerState", "RetailerCity")
);

CREATE TABLE "Inventory"
(
    "ProductID"      bigint REFERENCES "Products",
    "RetailerID"     bigint REFERENCES "Retailers",
    PRIMARY KEY ("RetailerID", "ProductID"),
    "QuantityOnHand" integer NOT NULL,
    "ReorderLevel"   integer NOT NULL
);

CREATE TABLE "Locations"
(
    "LocationID" bigint PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    "City"       text NOT NULL,
    "State"      text NOT NULL,
    "ZipCode"    text,
    "Country"    text NOT NULL,
    "Region"     text
);

CREATE TABLE "Customers"
(
    "CustomerID" bigint PRIMARY KEY,
    "LocationID" bigint REFERENCES "Locations" NOT NULL,
    "Market"     text                               NOT NULL,
    "FirstName"  text                               NOT NULL,
    "LastName"   text,
    "Segment"    text                               NOT NULL
);

CREATE TABLE "Failed Orders"
(
    "OrderID" bigint PRIMARY KEY,
    "FailureReason" text
)