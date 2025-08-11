-- Grain: one row per review_id
-- dim_products, dim_customers and dim_date
with fact_responses as 
(
    select
         response_id,
         r.review_id as review_id,
         d.date_id as date_id,
         responder_type,
         response_date,
         response_text
    from {{ref("stg_review_responses")}} as rr
    left join {{("stg_reviews")}} as r on rr.review_id = r.review_id
    join {{ref ('dim_date')}} as d
    on rr.response_date = d.date_day

)
select * from fact_responses