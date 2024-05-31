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
        m."CalculationId" IN (
            SELECT
                calculationId
            FROM
                cal))
    SELECT
        SUM(pg_column_size(m)) AS filesize_metrics
FROM
    "CalculationMetrics" AS m
WHERE
    m."CalculationId" IN (
        SELECT
            calculationId
        FROM
            cal_metrics)
