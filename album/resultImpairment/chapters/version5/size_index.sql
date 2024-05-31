WITH cal AS (
    SELECT c."CalculationId" AS calculationid
    FROM
        "Calculations" AS c
    WHERE
        c."Data"::json ->> 'calculationSpecVersion' = '5'
),

SELECT SUM(PG_COLUMN_SIZE(t.*)) AS filesize
-- 'Calculations' AS table_name,
--,
-- COUNT(*) AS row_count
FROM
    "Calculations" AS t
WHERE
    t."CalculationId" IN (SELECT calculationid FROM cal)
