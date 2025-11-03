WITH base AS (
  SELECT
    store_id,
    store_name,
    store_city,
    store_state,
    DATE_TRUNC(order_date, MONTH) AS month_start,
    EXTRACT(YEAR FROM order_date) AS year,
    EXTRACT(MONTH FROM order_date) AS month_num,
    order_id,
    customer_id,
    quantity,
    discount,
    unit_price_net,
    line_revenue_net
  FROM {{ ref('int__sales_enriched') }}
),
agg AS (
  SELECT
    store_id,
    store_name,
    store_city,
    store_state,
    month_start,
    year,
    month_num,
    COUNT(DISTINCT order_id) AS orders,
    COUNT(DISTINCT customer_id) AS customers,
    SUM(quantity) AS units_sold,
    SUM(line_revenue_net) AS revenue_net,
    SUM(discount * line_revenue_net) AS sum_disc_rev,
    SUM(unit_price_net * quantity) AS sum_upn_qty,
    SUM(quantity) AS sum_qty
  FROM base
  GROUP BY 1,2,3,4,5,6,7
)
SELECT
  store_id,
  store_name,
  store_city,
  store_state,
  month_start,
  year,
  month_num,
  orders,
  customers,
  units_sold,
  revenue_net,
  SAFE_DIVIDE(revenue_net, orders) AS avg_order_value,
  SAFE_DIVIDE(sum_disc_rev, NULLIF(revenue_net, 0)) AS avg_discount_wtd,
  SAFE_DIVIDE(sum_upn_qty, NULLIF(sum_qty, 0)) AS avg_unit_price_net
FROM agg
ORDER BY store_name, month_start
