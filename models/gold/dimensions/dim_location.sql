WITH src AS (
  SELECT NULLIF(TRIM(location), '') AS location_raw
  FROM {{ ref('stg_customers') }}
),

norm AS (
  SELECT
    UPPER(location_raw) AS location_up,
    COALESCE(LENGTH(location_raw) - LENGTH(REPLACE(location_raw, ',', '')), 0) AS comma_cnt
  FROM src
  WHERE location_raw IS NOT NULL
),

parts AS (
  SELECT
    TRIM(SPLIT_PART(location_up, ',', 1)) AS part1,
    TRIM(SPLIT_PART(location_up, ',', 2)) AS part2,
    TRIM(SPLIT_PART(location_up, ',', 3)) AS part3,
    comma_cnt
  FROM norm
),

mapped AS (
  SELECT
    /* If 2+ commas: "City, State, Country"
       If 1 comma:   "City, Country"
       Else:         "Country" only
    */
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
),

dedup AS (
  SELECT DISTINCT
    country, state_or_region, city
  FROM mapped
)

SELECT
  LOWER(TO_VARCHAR(
    MD5(CONCAT(
      COALESCE(country,''), '|', COALESCE(state_or_region,''), '|', COALESCE(city,'')
    ))
  )) AS location_id,
  country,
  state_or_region,
  city,
  CURRENT_TIMESTAMP() AS _loaded_at
FROM dedup
