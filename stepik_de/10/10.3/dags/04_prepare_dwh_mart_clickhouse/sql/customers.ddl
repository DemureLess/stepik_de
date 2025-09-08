CREATE TABLE IF NOT EXISTS dwh.customers_parsed
(
    customer_id String,
    first_name String,
    last_name String,
    email String,
    phone String,
    birth_date Date,
    gender String,
    registration_date DateTime64(6),
    is_loyalty_member UInt8,
    loyalty_card_number String,

    purchase_country String,
    purchase_city String,
    purchase_street String,
    purchase_house String,
    purchase_postal_code String,
    purchase_latitude Float64,
    purchase_longitude Float64,

    delivery_country String,
    delivery_city String,
    delivery_street String,
    delivery_house String,
    delivery_apartment String,
    delivery_postal_code String,

    preferred_language String,
    preferred_payment_method String,
    receive_promotions UInt8,

    row_hash UInt64
) ENGINE = MergeTree
ORDER BY (row_hash);
