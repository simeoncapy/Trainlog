SELECT 
    user_id,
    SUM(carbon) as total_carbon,
    SUM(trip_length) as total_distance,
    COUNT(*) as trips,
    MAX(last_modified) as last_modified
FROM trips
WHERE user_id = ANY(:user_ids)
AND carbon IS NOT NULL
GROUP BY user_id