with raw as (
    select * from {{source('bronze', 'review_analytics')}}
)


select * from raw