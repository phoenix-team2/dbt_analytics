WITH raw AS (
  SELECT *
  FROM {{ source('bronze', 'reviews') }}
),

dedup as
    (
        select
        review_id, 
        customer_id,
        product_id,
        cast(review_date as date)   as review_date,     
        initcap(trim(title))        as review_title,
        trim(review_text)           as review_text,
        cast (rating as int)        as rating,
        total_votes,
        helpful_votes,
        sentiment_score,
        initcap(verified_purchase)   as verified_purchase,
        row_number() over (partition by customer_id 
                            order by  _AIRBYTE_EXTRACTED_AT desc,
                            coalesce(_AIRBYTE_GENERATION_ID, 0)desc

                            ) as row_num
    from raw  
    )
       
select 
    review_id, 
    customer_id,
    product_id,
    review_date,
    review_title, 
    review_text,
    rating,
    total_votes,
    helpful_votes,
    sentiment_score, 
    verified_purchase,
    current_timestamp()  as loaded_at
from dedup
where row_num = 1       
                                        
