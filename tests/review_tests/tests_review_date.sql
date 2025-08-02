 
SELECT review_date
from {{ref("stg_reviews")}}
where review_date  != cast(review_date as date) 
or length(review_date) != 10 or review_date > getdate()
or review_date <1900-01-01