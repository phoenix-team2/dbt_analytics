   {{ config(materialized='test') }}
select rating
from {{ref("stg_reviews")}}
where rating < 0 or rating >5