-- Grain: one row per review_id
-- Enriche reviews with product_id, customer_id and date_id forming a relationship with
-- dim_products, dim_customers and dim_date

with reviews as (
    select * from {{ref ('stg_reviews')}}
),
fact as (
select 
    r.review_id                             as review_id,
    p.product_id                            as product_id,
    cus.customer_id                         as customer_id,
    d.date_id                               as date_id,
    r.review_title                          as review_title,
    r.review_date                           as review_date,
    r.rating                                as rating,
    r.total_votes                           as total_votes,
    r.helpful_votes                         as helpful_votes,
    r.sentiment_score                       as sentiment_score,
    r.verified_purchase                     as verified_purchase
from reviews as r
join {{ref('dim_customers')}} as cus
    on r.customer_id = cus.customer_id
join {{ref ('dim_products')}} as p
    on r.product_id = p.product_id
join {{ref ('dim_date')}} as d
    on r.review_date = d.date_day
)

select
    review_id,
    product_id,
    customer_id,
    date_id,
    review_title,
    review_date,
    rating,
    total_votes,
    helpful_votes,
    sentiment_score,
    verified_purchase
from fact