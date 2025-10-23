INSERT INTO paths (trip_id, path)
VALUES (:trip_id, ST_GeomFromText(:wkt, 4326))
ON CONFLICT (trip_id) DO UPDATE
SET path = EXCLUDED.path;
