CREATE MATERIALIZED VIEW IF NOT EXISTS dwh.products_parsed_mv
TO dwh.products_parsed AS
WITH parsed_data AS (
    SELECT
        nullIf(lowerUTF8(JSON_VALUE(full_data, '$.id')), '') AS product_id,
        nullIf(lowerUTF8(JSON_VALUE(full_data, '$.name')), '') AS name,
        nullIf(lowerUTF8(JSON_VALUE(full_data, '$.group')), '') AS group_name,
        nullIf(lowerUTF8(JSON_VALUE(full_data, '$.description')), '') AS description,
        toFloat64OrNull(JSON_VALUE(full_data, '$.kbju.calories')) AS calories,
        toFloat64OrNull(JSON_VALUE(full_data, '$.kbju.protein')) AS protein,
        toFloat64OrNull(JSON_VALUE(full_data, '$.kbju.fat')) AS fat,
        toFloat64OrNull(JSON_VALUE(full_data, '$.kbju.carbohydrates')) AS carbohydrates,
        toFloat64OrNull(JSON_VALUE(full_data, '$.price')) AS price,
        nullIf(lowerUTF8(JSON_VALUE(full_data, '$.unit')), '') AS unit,
        nullIf(lowerUTF8(JSON_VALUE(full_data, '$.origin_country')), '') AS origin_country,
        toUInt16OrNull(JSON_VALUE(full_data, '$.expiry_days')) AS expiry_days,
        JSONExtractBool(full_data, 'is_organic') AS is_organic,
        nullIf(lowerUTF8(JSON_VALUE(full_data, '$.barcode')), '') AS barcode,
        nullIf(lowerUTF8(JSON_VALUE(full_data, '$.manufacturer.name')), '') AS manufacturer_name,
        nullIf(lowerUTF8(JSON_VALUE(full_data, '$.manufacturer.country')), '') AS manufacturer_country,
        nullIf(lowerUTF8(JSON_VALUE(full_data, '$.manufacturer.website')), '') AS manufacturer_website,
        nullIf(lowerUTF8(JSON_VALUE(full_data, '$.manufacturer.inn')), '') AS manufacturer_inn,

        cityHash64(
            nullIf(lowerUTF8(JSON_VALUE(full_data, '$.id')), ''),
            nullIf(lowerUTF8(JSON_VALUE(full_data, '$.name')), ''),
            nullIf(lowerUTF8(JSON_VALUE(full_data, '$.group')), ''),
            nullIf(lowerUTF8(JSON_VALUE(full_data, '$.description')), ''),
            toFloat64OrNull(JSON_VALUE(full_data, '$.kbju.calories')),
            toFloat64OrNull(JSON_VALUE(full_data, '$.kbju.protein')),
            toFloat64OrNull(JSON_VALUE(full_data, '$.kbju.fat')),
            toFloat64OrNull(JSON_VALUE(full_data, '$.kbju.carbohydrates')),
            toFloat64OrNull(JSON_VALUE(full_data, '$.price')),
            nullIf(lowerUTF8(JSON_VALUE(full_data, '$.unit')), ''),
            nullIf(lowerUTF8(JSON_VALUE(full_data, '$.origin_country')), ''),
            toUInt16OrNull(JSON_VALUE(full_data, '$.expiry_days')),
            JSONExtractBool(full_data, 'is_organic'),
            nullIf(lowerUTF8(JSON_VALUE(full_data, '$.barcode')), ''),
            nullIf(lowerUTF8(JSON_VALUE(full_data, '$.manufacturer.name')), ''),
            nullIf(lowerUTF8(JSON_VALUE(full_data, '$.manufacturer.country')), ''),
            nullIf(lowerUTF8(JSON_VALUE(full_data, '$.manufacturer.website')), ''),
            nullIf(lowerUTF8(JSON_VALUE(full_data, '$.manufacturer.inn')), '')
        ) AS row_hash
    FROM default.products_data
    WHERE isValidJSON(full_data) AND full_data != ''
)
SELECT DISTINCT
    product_id,
    name,
    group_name,
    description,
    calories,
    protein,
    fat,
    carbohydrates,
    price,
    unit,
    origin_country,
    expiry_days,
    is_organic,
    barcode,
    manufacturer_name,
    manufacturer_country,
    manufacturer_website,
    manufacturer_inn,
    row_hash
FROM parsed_data
LEFT ANTI JOIN dwh.products_parsed ON parsed_data.row_hash = dwh.products_parsed.row_hash;
