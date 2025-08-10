-- Grain: one row per customer_id
-- Enrich customers with a location_id FK by parsing the same way as dim_location

WITH c AS (
  SELECT
    customer_id,
    email,
    first_name,
    last_name,
    join_date,
    reviewer_level,
    NULLIF(TRIM(location), '') AS location_raw
  FROM {{ ref('stg_customers') }}
),

norm AS (
  SELECT
    customer_id, email, first_name, last_name, join_date, reviewer_level,
    UPPER(location_raw) AS location_up,
    COALESCE(LENGTH(location_raw) - LENGTH(REPLACE(location_raw, ',', '')), 0) AS comma_cnt
  FROM c
),

parts AS (
  SELECT
    customer_id, email, first_name, last_name, join_date, reviewer_level,
    TRIM(SPLIT_PART(location_up, ',', 1)) AS part1,
    TRIM(SPLIT_PART(location_up, ',', 2)) AS part2,
    TRIM(SPLIT_PART(location_up, ',', 3)) AS part3,
    comma_cnt
  FROM norm
),

mapped AS (
  SELECT
    customer_id, email, first_name, last_name, join_date, reviewer_level,
    CASE WHEN comma_cnt >= 2 THEN part1
         WHEN comma_cnt = 1 THEN part1
         ELSE NULL END                            AS city,
    CASE WHEN comma_cnt >= 2 THEN part2
         WHEN comma_cnt = 1 THEN NULL
         ELSE NULL END                            AS state_or_region,
    CASE WHEN comma_cnt >= 2 THEN part3
         WHEN comma_cnt = 1 THEN part2
         ELSE part1 END                           AS country
  FROM parts
)

SELECT
  m.customer_id,
  m.email,
  m.first_name,
  m.last_name,
  m.join_date,
  m.reviewer_level,
  d.location_id,
  CURRENT_TIMESTAMP() AS _loaded_at
FROM mapped m
LEFT JOIN {{ ref('dim_location') }} d
  ON COALESCE(d.country,'') = COALESCE(m.country,'')
 AND COALESCE(d.state_or_region,'') = COALESCE(m.state_or_region,'')
 AND COALESCE(d.city,'') = COALESCE(m.city,'')
