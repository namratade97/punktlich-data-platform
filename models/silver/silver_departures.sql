-- silver_departures.sql
-- Reads Bronze Parquet files and creates a cleaned, deduplicated Silver layer

{{ config(materialized='table', schema='main') }}


with bronze as (

    -- Reading all Parquet files in the bronze folder
    select *
    from read_parquet('data/bronze/*.parquet')

),

parsed as (

    -- Parsing timestamps and cleaning up columns
    select
        trip_id,
        train,
        destination,
        path,
        try_cast(scheduled_time as timestamp) as scheduled_time,
        platform,
        delay,
        service_notices
    from bronze
),

deduped as (

    -- Keeping only the latest record for each trip_id
    select *
    from (
        select
            *,
            row_number() over (partition by trip_id order by scheduled_time desc) as rn
        from parsed
    ) t
    where rn = 1

),

final_enrichment as (
    select
        trip_id,
        train,
        -- Categorize the service
        case 
            when train LIKE 'ICE%' or train LIKE 'IC%' or train LIKE 'EC%' or train LIKE 'NJ%' or train LIKE 'RJ%' then 'Long Distance'
            when train LIKE 'RE%' or train LIKE 'RB%' or train LIKE 'OE%' then 'Regional'
            when train LIKE 'FEX%' then 'Airport Express'
            when train LIKE 'Unknown%' then 'S-Bahn/Local'
            else 'Other'
        end as service_type,
        
        destination,
        scheduled_time,
        
        -- Time features for later analysis
        date_part('hour', scheduled_time) as scheduled_hour,
        dayname(scheduled_time) as day_of_week,
        
        delay,
        
        -- Punctuality Bucket
        case 
            when delay <= 0 then 'On Time'
            when delay < 6 then 'Small Delay'
            else 'Late'
        end as status,
        
        --  Cleaning up strings
        upper(platform) as platform,
        service_notices,
        path
    from deduped
)


select * from final_enrichment order by scheduled_time limit 500