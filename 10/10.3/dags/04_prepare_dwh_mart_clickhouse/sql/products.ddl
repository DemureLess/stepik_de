CREATE TABLE IF NOT EXISTS dwh.products_parsed
(
    product_id String,
    name String,
    group_name String,
    description String,
    calories Float64,
    protein Float64,
    fat Float64,
    carbohydrates Float64,
    price Float64,
    unit String,
    origin_country String,
    expiry_days UInt16,
    is_organic UInt8,
    barcode String,
    manufacturer_name String,
    manufacturer_country String,
    manufacturer_website String,
    manufacturer_inn String,

    row_hash UInt64
) ENGINE = MergeTree
ORDER BY (row_hash);
