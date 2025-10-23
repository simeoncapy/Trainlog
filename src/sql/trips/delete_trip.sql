-- Delete the path first (due to foreign key constraint)
DELETE FROM paths WHERE trip_id = :trip_id;

-- Delete the trip
DELETE FROM trips WHERE trip_id = :trip_id;
