CREATE TABLE products
(
    product_id    bigint PRIMARY KEY,
    product_name  text           NOT NULL,
    category      text           NOT NULL,
    product_price DECIMAL(10, 2) NOT NULL
);

CREATE TABLE retailers
(
    retailer_id      bigint PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    retailer_name    text NOT NULL,
    retailer_country text NOT NULL,
    retailer_state   text NOT NULL,
    retailer_city    text NOT NULL,
    UNIQUE (retailer_country, retailer_state, retailer_city)
);

CREATE TABLE inventory
(
    product_id       bigint references products,
    retailer_id      bigint references retailers,
    PRIMARY KEY (retailer_id, product_id),
    quantity_on_hand integer NOT NULL,
    reorder_level    integer NOT NULL
);

CREATE TABLE locations
(
    location_id bigint PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    city        text NOT NULL,
    state       text NOT NULL,
    zip_code    text,
    country     text NOT NULL,
    region      text
);

CREATE TABLE customers
(
    customer_id bigint PRIMARY KEY,
    location_id bigint references locations NOT NULL,
    market      text                        NOT NULL,
    first_name  text                        NOT NULL,
    last_name   text,
    segment     text                        NOT NULL
);
