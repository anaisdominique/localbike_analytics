SELECT
  brand_id,
  TRIM(brand_name) AS brand_name
FROM {{ source('localbike', 'brands') }}
