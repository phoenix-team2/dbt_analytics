WITH base AS (
  SELECT
    product_id,
    product_name,
    brand,
    category,
    price,
    launch_date,
    _loaded_at
  FROM    
  {{ref("stg_products")}} 
),

dedup AS (
  SELECT
    *,
    row_number() OVER (PARTITION BY product_id ORDER BY _loaded_at DESC) AS rn
  FROM
  base
)

SELECT
  product_id,
  product_name,
  brand,
  category,
  price,
  launch_date
FROM
  dedup
WHERE rn =1



