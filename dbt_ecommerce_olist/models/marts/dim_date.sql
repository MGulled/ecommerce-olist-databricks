with date_spine as (
    select explode(sequence(to_date('2016-01-01'), to_date('2019-12-31'), interval 1 day)) as date_day
)
select
    date_day,
    year(date_day)                 as year,
    month(date_day)                as month,
    day(date_day)                  as day,
    dayofweek(date_day)            as day_of_week,
    date_format(date_day, 'EEEE')  as day_name,
    date_format(date_day, 'MMMM')  as month_name,
    quarter(date_day)              as quarter
from date_spine