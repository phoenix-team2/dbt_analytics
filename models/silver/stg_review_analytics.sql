with raw as (
    select * from {{source('bronze', 'review_analytics')}}
),

stg_review_analytics as (
    select
        product_id,
        cast(one_star as integer)                       as one_star_review,
        cast(two_star as integer)                       as two_star_review,
        cast(three_star as integer)                     as three_star_review,
        cast(four_star as integer)                      as four_star_review,
        cast(five_star as integer)                      as five_star_review,
        cast(sentiment_avg as decimal(4,3))             as average_sentiment,
        cast(total_reviews as integer)                  as total_reviews,
        cast(average_rating as decimal(3,2))            as average_rating
    from raw
)


select 
    product_id,
    one_star_review,
    two_star_review,
    three_star_review,
    four_star_review,
    five_star_review,
    total_reviews,
    average_sentiment,
    average_rating,
    current_timestamp() as _loaded_at
from stg_review_analytics