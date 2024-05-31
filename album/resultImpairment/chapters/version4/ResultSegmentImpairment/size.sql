WITH cal AS (
    SELECT
        c."CalculationId" AS calculationId
    FROM
        "Calculations" c
    WHERE
        c."Data"::json ->> 'calculationSpecVersion' = '4'
),
cal_metrics AS (
    SELECT
        m."CalculationId" AS calculationId
    FROM
        "ResultSegmentImpairment" m
    WHERE
        m."CalculationId" IN (
            SELECT
                calculationId
            FROM
                cal))
    SELECT
        SUM(pg_column_size(m)) AS filesize_metrics
FROM
    "ResultSegmentImpairment" AS m
WHERE
    m."CalculationId" IN (
        SELECT
            calculationId
        FROM
            cal_metrics)
