WITH joined AS (
  SELECT
    o.order_id,
    o.order_date,
    o.store_id,
    s.store_name,
    s.city AS store_city,
    s.state AS store_state,
    o.customer_id,
    c.city AS customer_city,
    c.state AS customer_state,
    i.product_id,
    p.product_name,
    p.category_id,
    cat.category_name,
    p.brand_id,
    b.brand_name,
    p.model_year,
    i.quantity,
    i.list_price,
    i.discount,
    i.unit_price_net,
    i.line_revenue_net
  FROM {{ ref('stg__orders') }} AS o
  JOIN {{ ref('stg__order_items') }} AS i ON o.order_id = i.order_id
  JOIN {{ ref('stg__products') }} AS p ON i.product_id = p.product_id
  LEFT JOIN {{ ref('stg__categories') }} AS cat ON p.category_id = cat.category_id
  LEFT JOIN {{ ref('stg__brands') }} AS b ON p.brand_id = b.brand_id
  JOIN {{ ref('stg__stores') }} AS s ON o.store_id = s.store_id
  LEFT JOIN {{ ref('stg__customers') }} AS c ON o.customer_id = c.customer_id
)
SELECT *
FROM joined
WHERE quantity > 0
