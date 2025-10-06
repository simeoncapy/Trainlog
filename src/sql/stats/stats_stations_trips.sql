{base_filter}
{time_categories}

, stations AS (
    SELECT origin_station AS station, is_past, is_planned_future, is_future
    FROM time_categories
    UNION ALL
    SELECT destination_station AS station, is_past, is_planned_future, is_future
    FROM time_categories
)
SELECT 
    station,
    SUM(is_past) AS "past",
    SUM(is_planned_future) AS "plannedFuture",
    SUM(is_past + is_planned_future + is_future) AS "count"
FROM stations
GROUP BY station
ORDER BY count DESC
LIMIT 10
