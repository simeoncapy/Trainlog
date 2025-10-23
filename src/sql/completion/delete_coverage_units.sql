WITH deleted AS (
    DELETE FROM coverage_unit
    WHERE unit_id = ANY(:unit_ids::bigint[])
    RETURNING area_m2
)
SELECT COALESCE(SUM(area_m2), 0) as deleted_area_m2
FROM deleted;
