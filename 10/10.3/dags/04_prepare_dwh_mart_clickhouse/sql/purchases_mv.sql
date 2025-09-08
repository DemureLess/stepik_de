CREATE MATERIALIZED VIEW IF NOT EXISTS dwh.purchases_parsed_mv
TO dwh.purchases_parsed AS
WITH parsed_data AS (
    SELECT
        nullIf(lowerUTF8(JSON_VALUE(full_data, '$.purchase_id')), '') AS purchase_id,

        nullIf(lowerUTF8(JSON_VALUE(full_data, '$.customer.customer_id')), '') AS customer_id,
        nullIf(lowerUTF8(JSON_VALUE(full_data, '$.customer.first_name')), '') AS customer_first_name,
        nullIf(lowerUTF8(JSON_VALUE(full_data, '$.customer.last_name')), '') AS customer_last_name,

        nullIf(lowerUTF8(JSON_VALUE(full_data, '$.store.store_id')), '') AS store_id,
        nullIf(lowerUTF8(JSON_VALUE(full_data, '$.store.store_name')), '') AS store_name,
        nullIf(lowerUTF8(JSON_VALUE(full_data, '$.store.store_network')), '') AS store_network,

        nullIf(lowerUTF8(JSON_VALUE(full_data, '$.store.location.country')), '') AS store_country,
        nullIf(lowerUTF8(JSON_VALUE(full_data, '$.store.location.city')), '') AS store_city,
        nullIf(lowerUTF8(JSON_VALUE(full_data, '$.store.location.street')), '') AS store_street,
        nullIf(lowerUTF8(JSON_VALUE(full_data, '$.store.location.house')), '') AS store_house,
        nullIf(lowerUTF8(JSON_VALUE(full_data, '$.store.location.postal_code')), '') AS store_postal_code,
        toFloat64OrNull(JSON_VALUE(full_data, '$.store.location.coordinates.latitude')) AS latitude,
        toFloat64OrNull(JSON_VALUE(full_data, '$.store.location.coordinates.longitude')) AS longitude,

        nullIf(lowerUTF8(JSONExtract(full_data, 'items', 'String')), '') AS items,

        toFloat64OrNull(JSON_VALUE(full_data, '$.total_amount')) AS total_amount,
        nullIf(lowerUTF8(JSON_VALUE(full_data, '$.payment_method')), '') AS payment_method,
        JSONExtractBool(full_data, 'is_delivery') AS is_delivery,

        nullIf(lowerUTF8(JSON_VALUE(full_data, '$.delivery_address.country')), '') AS delivery_country,
        nullIf(lowerUTF8(JSON_VALUE(full_data, '$.delivery_address.city')), '') AS delivery_city,
        nullIf(lowerUTF8(JSON_VALUE(full_data, '$.delivery_address.street')), '') AS delivery_street,
        nullIf(lowerUTF8(JSON_VALUE(full_data, '$.delivery_address.house')), '') AS delivery_house,
        nullIf(lowerUTF8(JSON_VALUE(full_data, '$.delivery_address.apartment')), '') AS delivery_apartment,
        nullIf(lowerUTF8(JSON_VALUE(full_data, '$.delivery_address.postal_code')), '') AS delivery_postal_code,

        toDateTime64OrNull(JSON_VALUE(full_data, '$.purchase_datetime'), 6) AS purchase_datetime,

        cityHash64(
            nullIf(lowerUTF8(JSON_VALUE(full_data, '$.purchase_id')), ''),
            nullIf(lowerUTF8(JSON_VALUE(full_data, '$.customer.customer_id')), ''),
            nullIf(lowerUTF8(JSON_VALUE(full_data, '$.customer.first_name')), ''),
            nullIf(lowerUTF8(JSON_VALUE(full_data, '$.customer.last_name')), ''),
            nullIf(lowerUTF8(JSON_VALUE(full_data, '$.store.store_id')), ''),
            nullIf(lowerUTF8(JSON_VALUE(full_data, '$.store.store_name')), ''),
            nullIf(lowerUTF8(JSON_VALUE(full_data, '$.store.store_network')), ''),
            nullIf(lowerUTF8(JSON_VALUE(full_data, '$.store.location.country')), ''),
            nullIf(lowerUTF8(JSON_VALUE(full_data, '$.store.location.city')), ''),
            nullIf(lowerUTF8(JSON_VALUE(full_data, '$.store.location.street')), ''),
            nullIf(lowerUTF8(JSON_VALUE(full_data, '$.store.location.house')), ''),
            nullIf(lowerUTF8(JSON_VALUE(full_data, '$.store.location.postal_code')), ''),
            toFloat64OrNull(JSON_VALUE(full_data, '$.store.location.coordinates.latitude')),
            toFloat64OrNull(JSON_VALUE(full_data, '$.store.location.coordinates.longitude')),
            nullIf(lowerUTF8(JSONExtract(full_data, 'items', 'String')), ''),
            toFloat64OrNull(JSON_VALUE(full_data, '$.total_amount')),
            nullIf(lowerUTF8(JSON_VALUE(full_data, '$.payment_method')), ''),
            JSONExtractBool(full_data, 'is_delivery'),
            nullIf(lowerUTF8(JSON_VALUE(full_data, '$.delivery_address.country')), ''),
            nullIf(lowerUTF8(JSON_VALUE(full_data, '$.delivery_address.city')), ''),
            nullIf(lowerUTF8(JSON_VALUE(full_data, '$.delivery_address.street')), ''),
            nullIf(lowerUTF8(JSON_VALUE(full_data, '$.delivery_address.house')), ''),
            nullIf(lowerUTF8(JSON_VALUE(full_data, '$.delivery_address.apartment')), ''),
            nullIf(lowerUTF8(JSON_VALUE(full_data, '$.delivery_address.postal_code')), ''),
            toDateTime64OrNull(JSON_VALUE(full_data, '$.purchase_datetime'), 6)
        ) AS row_hash
    FROM default.purchases_data
    WHERE isValidJSON(full_data) AND full_data != ''
)
SELECT DISTINCT
    purchase_id,
    customer_id,
    customer_first_name,
    customer_last_name,
    store_id,
    store_name,
    store_network,
    store_country,
    store_city,
    store_street,
    store_house,
    store_postal_code,
    latitude,
    longitude,
    items,
    total_amount,
    payment_method,
    is_delivery,
    delivery_country,
    delivery_city,
    delivery_street,
    delivery_house,
    delivery_apartment,
    delivery_postal_code,
    purchase_datetime,
    row_hash
FROM parsed_data
LEFT ANTI JOIN dwh.purchases_parsed ON parsed_data.row_hash = dwh.purchases_parsed.row_hash;