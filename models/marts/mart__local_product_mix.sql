WITH by_cat AS (
  SELECT
    store_id,
    store_name,
    store_city,
    store_state,
    COALESCE(category_name, CONCAT('cat_', CAST(category_id AS STRING))) AS category_name,
    category_id,
    SUM(quantity) AS units_sold,
    SUM(line_revenue_net) AS revenue_net
  FROM {{ ref('int__sales_enriched') }}
  GROUP BY 1,2,3,4,5,6
),
with_store_sum AS (
  SELECT
    bc.*,
    SUM(revenue_net) OVER (PARTITION BY store_id) AS store_revenue
  FROM by_cat bc
)
SELECT
  store_id,
  store_name,
  store_city,
  store_state,
  category_name,
  category_id,
  units_sold,
  revenue_net,
  SAFE_DIVIDE(revenue_net, NULLIF(store_revenue, 0)) AS revenue_share_in_store,
  DENSE_RANK() OVER (PARTITION BY store_id ORDER BY revenue_net DESC) AS category_rank_in_store
FROM with_store_sum
ORDER BY store_name, category_rank_in_store
