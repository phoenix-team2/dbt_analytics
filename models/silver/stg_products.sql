with raw as (
    select * from {{source ('bronze', 'products')}}
), 
stg_prod as (
    select 
    product_id                       as product_id,
    initcap(trim(product_name))      as product_name, --capitalize the first letter in this column and clear white spaces
    initcap(trim(brand))             as brand,  
    initcap(trim(category))          as category,
    cast(price as decimal(18,2))     as price,
    cast(launch_date as date)        as launch_date,
    total_reviews                    as total_reviews,
    average_rating                   as average_rating,
    row_number() 
    over(partition by product_id
    order by _airbyte_extracted_at desc,
    coalesce(_airbyte_generation_id, 0) desc
    ) as rn  -- deduplicates by product_id
    from raw
)

select
    product_id,
    product_name,
    brand,
    category,
    price,
    launch_date,
    total_reviews,
    average_rating,
    current_timestamp()     as _loaded_at
from stg_prod
where rn = 1