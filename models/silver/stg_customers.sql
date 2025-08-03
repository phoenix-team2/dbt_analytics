WITH raw AS (
  SELECT *
  FROM {{ source('bronze', 'customers') }}
),

dedup AS (
  SELECT
    CUSTOMER_ID                               AS customer_id,
    LOWER(TRIM(EMAIL))                        AS email,
    INITCAP(TRIM(FIRST_NAME))                 AS first_name,
    INITCAP(TRIM(LAST_NAME))                  AS last_name,
    CAST(JOIN_DATE AS DATE)                   AS join_date,        -- ← fixed here
    LOCATION                                  AS location,
    REVIEWER_LEVEL                            AS reviewer_level,
    HELPFUL_VOTES_RECEIVED                    AS helpful_votes_received,
    TOTAL_REVIEWS                             AS total_reviews,
    ROW_NUMBER() OVER (
      PARTITION BY CUSTOMER_ID
      ORDER BY 
        _AIRBYTE_EXTRACTED_AT DESC,
        COALESCE(_AIRBYTE_GENERATION_ID, 0) DESC
    )                                       AS rn
  FROM raw
)

SELECT
  customer_id,
  email,
  first_name,
  last_name,
  join_date,
  location,
  reviewer_level,
  helpful_votes_received,
  total_reviews,
  CURRENT_TIMESTAMP()                      AS _loaded_at
FROM dedup
WHERE rn = 1