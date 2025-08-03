WITH raw AS (
  SELECT *
  FROM {{ source('bronze', 'review_analytics') }}
),

dedup AS (
  SELECT
product_id,
cast(one_star as integer) as one_star,
cast(two_star as integer) as two_star,
cast(three_star as integer) as three_star,
cast(four_star as integer) as four_star,
cast(five_star as integer) as five_star,
cast(sentiment_avg as decimal(4,3))as sentiment_avg,
cast(total_reviews as integer) as total_reviews,
cast(average_rating as decimal(3,2))as average_rating,

  FROM raw
)

SELECT
  product_id,
  one_star,
  two_star,
  three_star,
  four_star
  five_star,
  sentiment_avg,
  total_reviews,
  average_rating,
  current_timestamp () as _loaded_at
FROM dedup
