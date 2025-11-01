SELECT
  store_id,
  store_name,
  store_city,
  store_state,
  COALESCE(category_name, CONCAT('cat_', CAST(category_id AS STRING))) AS category_name,
  SUM(quantity) AS units_sold,
  SUM(line_revenue_net) AS revenue_net,
  SAFE_DIVIDE(SUM(line_revenue_net),
              NULLIF(SUM(SUM(line_revenue_net)) OVER (PARTITION BY store_id), 0)) AS revenue_share_in_store,
  DENSE_RANK() OVER (PARTITION BY store_id ORDER BY SUM(line_revenue_net) DESC) AS category_rank_in_store
FROM {{ ref('int__sales_enriched') }}
GROUP BY store_id, store_name, store_city, store_state, category_name, category_id
ORDER BY store_name, category_rank_in_store
