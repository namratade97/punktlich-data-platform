{{ config(materialized='table', schema='gold') }}

with silver as (
    select * from {{ ref('silver_departures') }}
),

final as (
    select
        service_type,
        scheduled_hour,
        day_of_week,

        count(*) as total_trains,

        count_if(status != 'On Time') as delayed_trains,

        round(avg(delay), 1) as avg_delay_minutes,

        round(
            count_if(status = 'On Time') * 100.0 / count(*),
            1
        ) as punctuality_rate,

        round(
            count_if(status != 'On Time') * 100.0 / count(*),
            1
        ) as delay_rate,

        -- For Streamlit metric:
        count_if(service_notices LIKE '%Störung%') as total_disruptions,
        
        round(count_if(service_notices LIKE '%Störung%') * 100.0 / count(*), 1) as disruption_rate

    from silver
    group by all
)

select * from final
