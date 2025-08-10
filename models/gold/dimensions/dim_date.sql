with date_series as (
    {{ dbt_utils.date_spine(
        datepart = "day",
        start_date = "cast('2024-01-01' as date)",
        end_date = "current_date()"
    )}}
)
select 
cast(to_char(date_day, 'YYYYMMDD') as int)          as date_id,
date_day as date_actual,
extract(year from date_day)                         as year,
extract(month from date_day)                        as month,
extract(quarter from date_day)                      as quarter,
extract(iso_dow from date_day)                      as day_of_week,
extract(week from date_day)                         as week_of_year
from date_series