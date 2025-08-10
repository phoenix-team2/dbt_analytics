with reviews as (
    select * from {{ref ('stg_reviews')}}
)
select min(review_date) from reviews
select 
r.review_id,
r.product_id,
cus.customer_id,
r.review_title,
r.review_date,
r.rating,
r.total_votes,
r.helpful_votes,
r.sentiment_score,
r.verified_purchase,
from reviews as r
join {{ref('dim_customers')}} as cus
on r.customer_id = cus.customer_id