WITH base AS (
    SELECT *
    FROM
    {{ref("stg_review_analytics")}}
),

aggregated AS (
    SELECT
      product_id,
      SUM(total_reviews) AS total_reviews,
      CASE WHEN SUM(total_reviews) > 0
           THEN SUM(average_rating*total_reviews)/ SUM(total_reviews)
           ELSE NULL
      END AS average_rating,
      SUM(five_star_review) AS five_star,
      SUM(four_star_review) AS four_star,
      SUM(three_star_review) AS three_star,
      SUM(two_star_review) AS two_star,
      SUM(one_star_review) AS one_star,
      CASE WHEN SUM(total_reviews)> 0 
           THEN SUM(average_sentiment * total_reviews)/ SUM(total_reviews)
           ELSE NULL
      END AS average_sentiment,
      MAX(_loaded_at) AS latest_loaded_at
     FROM base
     GROUP BY product_id
)      

SELECT
    product_id,
    total_reviews,
    average_rating,
    five_star,
    four_star,
    three_star,
    two_star,
    one_star,
    average_sentiment
FROM aggregated