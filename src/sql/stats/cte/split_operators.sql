-- Recursively split comma-separated operators
WITH RECURSIVE operator_split AS (
    SELECT 
        trip_id,
        CASE
            WHEN operator IS NULL OR TRIM(operator) = '' THEN NULL
            WHEN POSITION(',' IN operator) > 0 THEN TRIM(SUBSTRING(operator, 1, POSITION(',' IN operator) - 1))
            ELSE TRIM(operator)
        END AS operator,
        CASE 
            WHEN operator IS NULL OR TRIM(operator) = '' THEN NULL
            WHEN POSITION(',' IN operator) > 0 THEN TRIM(SUBSTRING(operator FROM POSITION(',' IN operator) + 1))
            ELSE NULL
        END AS rest,
        trip_length, is_past, is_planned_future, is_future, is_project
    FROM time_categories
    WHERE is_project IS FALSE
    
    UNION ALL
    
    SELECT
        trip_id,
        CASE
            WHEN rest IS NULL OR TRIM(rest) = '' THEN NULL
            WHEN POSITION(',' IN rest) > 0 THEN TRIM(SUBSTRING(rest, 1, POSITION(',' IN rest) - 1))
            ELSE TRIM(rest)
        END,
        CASE 
            WHEN rest IS NULL OR TRIM(rest) = '' THEN NULL
            WHEN POSITION(',' IN rest) > 0 THEN TRIM(SUBSTRING(rest FROM POSITION(',' IN rest) + 1))
            ELSE NULL
        END,
        trip_length, is_past, is_planned_future, is_future, is_project
    FROM operator_split
    WHERE rest IS NOT NULL AND TRIM(rest) != ''
)
SELECT * FROM operator_split
WHERE operator IS NOT NULL