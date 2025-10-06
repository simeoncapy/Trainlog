{base_filter}
{time_categories}

SELECT 
    jsonb_build_array(
        LEAST(origin_station, destination_station), 
        GREATEST(origin_station, destination_station)
    )::text AS route,
    SUM(trip_length * is_past) AS "past",
    SUM(trip_length * is_planned_future) AS "plannedFuture",
    SUM(trip_length * is_future) AS "future",
    SUM(trip_length * (is_past + is_planned_future + is_future)) AS "count"
FROM time_categories
GROUP BY LEAST(origin_station, destination_station), GREATEST(origin_station, destination_station)
ORDER BY "count" DESC
LIMIT 10