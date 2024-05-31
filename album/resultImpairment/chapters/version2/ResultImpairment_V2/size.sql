WITH cal AS (
    SELECT
        c."CalculationId" AS calculationid
    FROM
        "Calculations" AS c
    WHERE
        c."Data"::json ->> 'calculationSpecVersion' = '2'
),
cal_metrics AS (
    SELECT
        m."CalculationId" AS calculationid
    FROM
        "ResultImpairment_V2" AS m
    WHERE
        m."CalculationId" IN (
            SELECT
                calculationid
            FROM
                cal))
SELECT
    SUM(PG_COLUMN_SIZE(m.*)) AS filesize_metrics
FROM
    "ResultImpairment_V2" AS m
WHERE
    m."CalculationId" IN (
        SELECT
            calculationid
        FROM
            cal_metrics);
