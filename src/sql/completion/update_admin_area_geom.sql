UPDATE admin_area a
SET geom = sub.geom
FROM (
    SELECT ST_Multi(ST_Union(cu.geom)) AS geom
    FROM coverage_unit cu
    JOIN admin_area a2 ON cu.admin_area_id = a2.admin_area_id
    WHERE a2.iso_code = :iso_code
) sub
WHERE a.iso_code = :iso_code;
