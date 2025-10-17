{base_filter}
{time_categories}

SELECT 
    jsonb_build_array(
        LEAST(origin_station, destination_station), 
        GREATEST(origin_station, destination_station)
    )::text AS route,
    SUM(is_past) AS "pastTrips",
    SUM(is_planned_future) AS "plannedFutureTrips",
    SUM(is_past + is_planned_future) AS "count",
    SUM(trip_length * is_past) AS "pastKm",
    SUM(trip_length * is_planned_future) AS "plannedFutureKm",
    SUM(trip_duration * is_past) AS "pastDuration",
    SUM(trip_duration * is_planned_future) AS "plannedFutureDuration",
    SUM(carbon * is_past) AS "pastCO2",
    SUM(carbon * is_planned_future) AS "plannedFutureCO2"
FROM time_categories
GROUP BY LEAST(origin_station, destination_station), GREATEST(origin_station, destination_station)
ORDER BY "count" DESC
LIMIT 10000;