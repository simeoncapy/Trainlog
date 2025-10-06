{base_filter}
{time_categories}
{split_operators}

SELECT 
    operator,
    SUM(is_past) AS "past",
    SUM(is_planned_future) AS "plannedFuture",
    SUM(is_past + is_planned_future) AS count
FROM split_operators
GROUP BY operator
ORDER BY count DESC
