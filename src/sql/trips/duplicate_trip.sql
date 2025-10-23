-- Duplicate the trip
INSERT INTO trips (
    trip_id,
    user_id,
    origin_station,
    destination_station,
    start_datetime,
    end_datetime,
    is_project,
    utc_start_datetime,
    utc_end_datetime,
    estimated_trip_duration,
    manual_trip_duration,
    trip_length,
    operator,
    countries,
    line_name,
    created,
    last_modified,
    trip_type,
    material_type,
    seat,
    reg,
    waypoints,
    notes,
    price,
    currency,
    ticket_id,
    purchase_date,
    carbon
)
SELECT
    :new_trip_id,
    user_id,
    origin_station,
    destination_station,
    start_datetime,
    end_datetime,
    is_project,
    utc_start_datetime,
    utc_end_datetime,
    estimated_trip_duration,
    manual_trip_duration,
    trip_length,
    operator,
    countries,
    line_name,
    created,
    last_modified,
    trip_type,
    material_type,
    seat,
    reg,
    waypoints,
    notes,
    price,
    currency,
    ticket_id,
    purchase_date,
    carbon
FROM trips
WHERE trip_id = :trip_id;

-- Duplicate the path
INSERT INTO paths (trip_id, path)
SELECT :new_trip_id, path
FROM paths
WHERE trip_id = :trip_id;
