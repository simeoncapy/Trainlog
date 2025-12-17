WITH base_filter AS (
    SELECT *, COALESCE(utc_start_datetime, start_datetime) AS filtered_datetime
    FROM trips
    WHERE (:tripType = 'combined' OR trip_type = :tripType)
    AND user_id = :user_id
    AND EXTRACT(YEAR FROM COALESCE(utc_start_datetime, start_datetime))::text = :year
    AND is_project = false
    AND COALESCE(utc_start_datetime, start_datetime) < NOW()
    AND countries IS NOT NULL
),
country_data AS (
    SELECT 
        jsonb_object_keys(countries::jsonb) AS country_code,
        trip_length,
        trip_id
    FROM base_filter
),
country_totals AS (
    SELECT 
        country_code,
        COUNT(DISTINCT trip_id) AS trips,
        SUM(trip_length) AS total_km
    FROM country_data
    GROUP BY country_code
    ORDER BY total_km DESC, trips DESC
)
SELECT 
    country_code,
    trips,
    total_km
FROM country_totals
