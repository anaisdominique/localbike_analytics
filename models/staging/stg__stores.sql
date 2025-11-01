SELECT
  store_id,
  store_name,
  phone,
  LOWER(email) AS email,
  street,
  city,
  state,
  zip_code
FROM {{ source('localbike', 'stores') }}
