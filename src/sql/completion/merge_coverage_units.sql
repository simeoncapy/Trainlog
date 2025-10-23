WITH units_to_merge AS (
    SELECT 
        cu.unit_id,
        cu.admin_area_id,
        cu.geom,
        cu.area_m2,
        cu.source_feature_id,
        cu.properties_json
    FROM coverage_unit cu
    WHERE cu.unit_id = ANY(:unit_ids::bigint[])
),
merged_geom AS (
    SELECT 
        MIN(admin_area_id) as admin_area_id,
        ST_Multi(ST_Union(geom)) as geom,
        SUM(area_m2) as total_area_m2,
        MIN(source_feature_id) as source_feature_id,
        jsonb_build_object('merged_from', array_agg(unit_id)) as properties_json
    FROM units_to_merge
),
deleted AS (
    DELETE FROM coverage_unit
    WHERE unit_id = ANY(:unit_ids::bigint[])
    RETURNING unit_id
),
inserted AS (
    INSERT INTO coverage_unit (admin_area_id, source_feature_id, geom, properties_json)
    SELECT 
        admin_area_id,
        source_feature_id || '_merged',
        geom,
        properties_json
    FROM merged_geom
    RETURNING unit_id, area_m2
)
SELECT unit_id, area_m2
FROM inserted;
