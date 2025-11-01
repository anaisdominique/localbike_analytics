SELECT
  customer_id,
  first_name,
  last_name,
  phone,
  LOWER(email) AS email,
  street,
  city,
  state,
  zip_code
FROM {{ source('localbike', 'customers') }}
