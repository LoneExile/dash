WITH cal AS (
    SELECT
        c."CalculationId" AS calculationId
    FROM
        "Calculations" c
    WHERE
        c."Data"::json ->> 'calculationSpecVersion' = '2'
),
cal_metrics AS (
    SELECT
        m."CalculationId" AS calculationId
    FROM
        "ResultImpairment_V2" m
    WHERE
        m."CalculationId" IN (
            SELECT
                calculationId
            FROM
                cal))
    SELECT
        SUM(pg_column_size(m)) AS filesize_metrics
FROM
    "ResultImpairment_V2" AS m
WHERE
    m."CalculationId" IN (
        SELECT
            calculationId
        FROM
            cal_metrics)
