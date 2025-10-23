WITH units AS (
    SELECT 
        cu.unit_id,
        cu.source_feature_id,
        cu.area_m2,
        ST_AsGeoJSON(cu.geom)::json as geometry,
        cu.properties_json
    FROM coverage_unit cu
    JOIN admin_area a ON cu.admin_area_id = a.admin_area_id
    WHERE a.iso_code = :iso_code
),
total AS (
    SELECT COALESCE(SUM(cu.area_m2), 0) as total_area
    FROM coverage_unit cu
    JOIN admin_area a ON cu.admin_area_id = a.admin_area_id
    WHERE a.iso_code = :iso_code
)
SELECT json_build_object(
    'type', 'FeatureCollection',
    'total_area_m2', (SELECT total_area FROM total),
    'features', COALESCE(json_agg(
        json_build_object(
            'type', 'Feature',
            'properties', (properties_json || jsonb_build_object(
                'id', unit_id,
                'area_m2', area_m2
            ))::json,
            'geometry', geometry
        )
    ), '[]'::json)
) as geojson
FROM units;