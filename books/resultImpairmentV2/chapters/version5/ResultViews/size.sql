WITH result_views AS (
    SELECT "CalculationId" AS calculationid
    FROM
        "ResultViews"
    WHERE
        "CalculationId" IN ({{ ID_LIST }})
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
