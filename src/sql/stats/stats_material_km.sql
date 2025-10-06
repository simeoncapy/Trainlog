{base_filter}
{time_categories}
{split_material}

SELECT 
    CASE 
        WHEN :tripType IN ('air', 'helicopter') AND a.iata IS NOT NULL 
        THEN a.manufacturer || ' ' || a.model
        ELSE m.material_type
    END AS material,
    SUM(m.trip_length * m.is_past) AS "past",
    SUM(m.trip_length * m.is_planned_future) AS "plannedFuture",
    SUM(m.trip_length * (m.is_past + m.is_planned_future)) AS count
FROM split_material m
LEFT JOIN airliners a ON m.material_type = a.iata
GROUP BY material
ORDER BY count DESC
