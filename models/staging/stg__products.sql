SELECT
  product_id,
  TRIM(product_name) AS product_name,
  brand_id,
  category_id,
  model_year,
  CAST(list_price AS NUMERIC) AS list_price
FROM {{ source('localbike', 'products') }}
