SELECT 
    COUNT(trip_id) AS trips,
    CAST(SUM(trip_length)/1000 AS INT) AS km
FROM trips
WHERE (:user_id IS NULL OR user_id = :user_id)
