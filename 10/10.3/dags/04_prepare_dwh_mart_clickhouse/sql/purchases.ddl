CREATE TABLE IF NOT EXISTS dwh.purchases_parsed
(
    purchase_id String,
    customer_id String,
    customer_first_name String,
    customer_last_name String,

    store_id String,
    store_name String,
    store_network String,

    store_country String,
    store_city String,
    store_street String,
    store_house String,
    store_postal_code String,
    latitude Float64,
    longitude Float64,

    items String,

    total_amount Float64,
    payment_method String,
    is_delivery UInt8,

    delivery_country String,
    delivery_city String,
    delivery_street String,
    delivery_house String,
    delivery_apartment String,
    delivery_postal_code String,

    purchase_datetime DateTime64(6),

    row_hash UInt64 
) ENGINE = MergeTree
ORDER BY (row_hash);