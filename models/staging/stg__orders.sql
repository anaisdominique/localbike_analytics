SELECT
  order_id,
  customer_id,
  store_id,
  staff_id,
  CAST(order_date AS DATE) AS order_date,
  CAST(required_date AS DATE) AS required_date,
  CAST(shipped_date AS DATE) AS shipped_date,
  order_status
FROM {{ source('localbike', 'orders') }}
WHERE order_date IS NOT NULL
