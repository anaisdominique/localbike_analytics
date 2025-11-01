SELECT
  order_id,
  item_id,
  product_id,
  quantity,
  CAST(list_price AS NUMERIC) AS list_price,
  COALESCE(CAST(discount AS NUMERIC), 0) AS discount,
  list_price * (1 - COALESCE(discount, 0)) AS unit_price_net,
  quantity * list_price * (1 - COALESCE(discount, 0)) AS line_revenue_net
FROM {{ source('localbike', 'order_items') }}
