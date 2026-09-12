-- Warehouse DDL (idempotent). Applied on first Postgres boot and on airflow-init.

CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS marts;

CREATE TABLE IF NOT EXISTS raw.events (
    id SERIAL PRIMARY KEY,
    event_type VARCHAR(50) NOT NULL,
    user_id INTEGER NOT NULL,
    product_id INTEGER,
    event_timestamp TIMESTAMP NOT NULL,
    source_file VARCHAR(255) NOT NULL UNIQUE,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS marts.cart_snapshot (
    user_id INTEGER NOT NULL,
    snapshot_hour TIMESTAMP NOT NULL,
    add_to_cart_count INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (user_id, snapshot_hour)
);

CREATE TABLE IF NOT EXISTS marts.quantity_of_purchases (
    operation_timestamp TIMESTAMP NOT NULL PRIMARY KEY,
    total_quantity NUMERIC(10, 2) NOT NULL DEFAULT 0
);
