{base_filter}
{time_categories}

SELECT 
    EXTRACT(YEAR FROM filtered_datetime)::text AS year,
    SUM(trip_length * is_past) AS "past",
    SUM(trip_length * is_planned_future) AS "plannedFuture",
    SUM(trip_length * is_future) AS "future"
FROM time_categories
WHERE EXTRACT(YEAR FROM filtered_datetime) > 1950
AND EXTRACT(YEAR FROM filtered_datetime) < 2100
GROUP BY year

UNION ALL

SELECT 
    'future' AS year,
    0 AS "past",
    0 AS "plannedFuture",
    SUM(trip_length * is_future) AS "future"
FROM time_categories
WHERE filtered_datetime IS NULL AND is_project = false
HAVING SUM(is_future) > 0

ORDER BY year
