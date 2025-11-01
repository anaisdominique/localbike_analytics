SELECT
  staff_id,
  TRIM(first_name) AS first_name,
  TRIM(last_name) AS last_name,
  LOWER(TRIM(email)) AS email,
  phone,
  active,
  store_id,
  manager_id
FROM {{ source('localbike', 'staffs') }}

