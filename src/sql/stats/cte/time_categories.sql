-- Categorize trips by time (past, plannedFuture, future)
SELECT *,
    CASE
        WHEN is_project = false 
        AND filtered_datetime IS NOT NULL 
        AND NOW() > filtered_datetime
        THEN 1 ELSE 0
    END AS is_past,
    CASE
        WHEN is_project = false 
        AND filtered_datetime IS NOT NULL 
        AND NOW() <= filtered_datetime
        THEN 1 ELSE 0
    END AS is_planned_future,
    CASE
        WHEN is_project = false AND filtered_datetime IS NULL
        THEN 1 ELSE 0
    END AS is_future
FROM base_filter
