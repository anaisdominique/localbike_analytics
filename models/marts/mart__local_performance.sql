SELECT
  store_id,
  store_name,
  store_city,
  store_state,
  DATE_TRUNC(order_date, MONTH) AS month_start,
  EXTRACT(YEAR FROM order_date) AS year,
  EXTRACT(MONTH FROM order_date) AS month_num,
  COUNT(DISTINCT order_id) AS orders,
  COUNT(DISTINCT customer_id) AS customers,
  SUM(quantity) AS units_sold,
  SUM(line_revenue_net) AS revenue_net,
  SAFE_DIVIDE(SUM(line_revenue_net), COUNT(DISTINCT order_id)) AS avg_order_value,
  SAFE_DIVIDE(SUM(discount * line_revenue_net), NULLIF(SUM(line_revenue_net), 0)) AS avg_discount_wtd,
  SAFE_DIVIDE(SUM(unit_price_net * quantity), NULLIF(SUM(quantity), 0)) AS avg_unit_price_net
FROM {{ ref('int__sales_enriched') }}
GROUP BY store_id, store_name, store_city, store_state, month_start, year, month_num
ORDER BY store_name, month_start
