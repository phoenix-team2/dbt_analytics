with date_spine as (
    {{ dbt_utils.date_spine(
        datepart = "day",
        start_date = "cast('2024-01-01' as date)",
        end_date = "current_date()"
    )}}
), 
data_attributes as (
    select 
        cast(to_char(date_day, 'YYYYMMDD') as int)          as date_id,
        date_day                                            as date_day,
        year(date_day)                                      as year,
        month(date_day)                                     as month,
        quarter(date_day)                                   as quarter,
        dayofweekiso(date_day)                              as day_of_week,
        weekiso(date_day)                                   as week_of_year
    from date_spine
)

select 
    date_id,
    date_day,
    year,
    month,
    quarter,
    day_of_week,
    week_of_year
from data_attributes
order by date_day