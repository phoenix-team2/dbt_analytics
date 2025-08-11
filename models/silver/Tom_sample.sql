select *
from ANALYTICS_TEAM2.bronze.review_analytics



WITH raw AS (
  SELECT *
  FROM {{ source('bronze', 'review_analytics') }}
)

