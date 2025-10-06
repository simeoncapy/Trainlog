SELECT DISTINCT trip_type
FROM trips 
WHERE (:user_id IS NULL OR user_id = :user_id)
