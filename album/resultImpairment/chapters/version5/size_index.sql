WITH cal AS (
    SELECT
        c."CalculationId" AS calculationId
    FROM
        "Calculations" c
    WHERE
        c."Data"::json ->> 'calculationSpecVersion' = '5'
),

cal_metrics AS (
    SELECT
        m."CalculationId" AS calculationId
    FROM
        "CalculationMetrics" m
    WHERE
        m."CalculationId" IN (SELECT calculationId FROM cal)
) --,

SELECT
    -- 'Calculations' AS table_name,
    SUM(pg_column_size(t)) AS filesize --,
    -- COUNT(*) AS row_count
FROM
    "Calculations" AS t
WHERE
    t."CalculationId" IN (SELECT calculationId FROM cal)
