-- Runs once, on an empty data volume (docker-entrypoint-initdb.d).

CREATE TABLE products (
    product_id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    unit_price NUMERIC(10,2) NOT NULL CHECK (unit_price > 0),
    opening_stock INT NOT NULL CHECK (opening_stock >= 0),
    reorder_point INT NOT NULL CHECK (reorder_point >= 0),
    reorder_qty INT NOT NULL CHECK (reorder_qty > 0)
);

CREATE TABLE orders (
    order_id BIGSERIAL PRIMARY KEY,
    channel TEXT NOT NULL CHECK (channel IN ('online', 'offline')),
    store_id INT,
    customer_id INT,
    product_id INT NOT NULL REFERENCES products (product_id),
    quantity INT NOT NULL CHECK (quantity > 0),
    amount NUMERIC(10,2) NOT NULL CHECK (amount >= 0),
    status TEXT NOT NULL DEFAULT 'placed' CHECK (status IN ('placed', 'cancelled')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    -- POS orders carry a store, online orders carry a customer
    CONSTRAINT orders_channel_ids CHECK (
        (channel = 'offline' AND store_id IS NOT NULL AND customer_id IS NULL) OR
        (channel = 'online' AND customer_id IS NOT NULL AND store_id IS NULL)
    )
);

CREATE TABLE restocks (
    restock_id BIGSERIAL PRIMARY KEY,
    product_id INT NOT NULL REFERENCES products (product_id),
    quantity INT NOT NULL CHECK (quantity > 0),
    status TEXT NOT NULL DEFAULT 'requested' CHECK (status IN ('requested', 'received')),
    requested_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    received_at TIMESTAMPTZ,
    CONSTRAINT restocks_received_at CHECK ((status = 'received') = (received_at IS NOT NULL))
);

-- At most one open restock per product: DB-level guard against double refills
CREATE UNIQUE INDEX restocks_one_open_per_product ON restocks (product_id) WHERE status = 'requested';

-- UPDATE/DELETE events carry the full old row for Debezium
ALTER TABLE products REPLICA IDENTITY FULL;
ALTER TABLE orders REPLICA IDENTITY FULL;
ALTER TABLE restocks REPLICA IDENTITY FULL;

INSERT INTO products (name, unit_price, opening_stock, reorder_point, reorder_qty) VALUES
    ('Espresso Beans 1kg', 24.90, 120, 40, 150),
    ('Oat Milk 1L', 3.50, 300, 40, 150),
    ('Ceramic Mug', 12.00, 150, 40, 150),
    ('Pour-over Kettle', 45.00, 100, 40, 150),
    ('Paper Filters (100)', 6.75, 250, 40, 150),
    ('Hand Grinder', 59.00, 100, 40, 150),
    ('Cold Brew Bottle', 18.50, 180, 40, 150),
    ('Travel Tumbler', 22.00, 200, 40, 150);
