WITH cal AS (
    SELECT c."CalculationId" AS calculationid
    FROM
        "Calculations" AS c
    WHERE
        c."Data"::json ->> 'calculationSpecVersion' = '5'
),

result_views AS (
    SELECT "CalculationId" AS calculationid
    FROM
        "ResultViews"
    WHERE
        "CalculationId" IN (SELECT calculationid FROM cal)
)

SELECT SUM(PG_COLUMN_SIZE(rv.*)) AS filesize
-- 'ResultViews' AS table_name,
-- SUM(PG_COLUMN_SIZE(rv.*)) AS filesize --,
--,
-- COUNT(*) AS row_count
FROM
    "ResultViews" AS rv
WHERE
    rv."CalculationId" IN (SELECT calculationid FROM result_views)
