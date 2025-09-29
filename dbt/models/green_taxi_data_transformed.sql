{{ config(
    materialized='table'
) }}

-- dbt automatically generates a log that tracks all info/errors/etc

SELECT
    t.*,
    
    -- calculating CO2 per trip in kilograms
    (t.trip_distance * v.co2_grams_per_mile / 1000.0) AS trip_co2_kgs,

    -- calculating the average mph of each trip using trip distance and trip duration in hours
    t.trip_distance / (EXTRACT(EPOCH FROM t.lpep_dropoff_datetime - t.lpep_pickup_datetime) / 3600.0) AS avg_mph,
    
    -- adding another column that tracks the hour of the day that the trip started
    EXTRACT(HOUR FROM t.lpep_pickup_datetime) AS hour_of_day,
    
    -- adding another column that tracks the day of the week of the trip (0 = Sunday, 6 = Saturday)
    EXTRACT(DOW FROM t.lpep_pickup_datetime) AS day_of_week,
    
    -- adding another column that tracks the week of the year of the trip (1-53)
    EXTRACT(WEEK FROM t.lpep_pickup_datetime) AS week_of_year,
    
    -- adding another column that tracks the month of the year of the trip (1-12)
    EXTRACT(MONTH FROM t.lpep_pickup_datetime) AS month_of_year

FROM {{ source('emissions', 'green_taxi_all_years_clean') }} t
LEFT JOIN {{ source('emissions', 'vehicle_emissions') }} v
    ON v.vehicle_type = 'green_taxi'
