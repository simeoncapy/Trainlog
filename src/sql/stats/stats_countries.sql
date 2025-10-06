{base_filter}
{time_categories}

SELECT 
    countries,
    is_past AS "past",
    is_planned_future AS "plannedFuture"
FROM time_categories
WHERE is_project IS FALSE
