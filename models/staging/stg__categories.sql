SELECT
  category_id,
  TRIM(category_name) AS category_name
FROM {{ source('localbike', 'categories') }}
