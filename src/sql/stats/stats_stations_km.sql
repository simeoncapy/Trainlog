{base_filter}
{time_categories}

, stations AS (
    SELECT origin_station AS station, trip_length, is_past, is_planned_future, is_future
    FROM time_categories
    UNION ALL
    SELECT destination_station AS station, trip_length, is_past, is_planned_future, is_future
    FROM time_categories
)
SELECT 
    station,
    SUM(trip_length * is_past) AS "past",
    SUM(trip_length * is_planned_future) AS "plannedFuture",
    SUM(trip_length * (is_past + is_planned_future + is_future)) AS "count"
FROM stations
GROUP BY station
ORDER BY count DESC
LIMIT 10
