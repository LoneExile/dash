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
        "CalculationTableRegistry"
    WHERE
        "CalculationId" IN (SELECT calculationid FROM cal)
)

SELECT SUM(PG_COLUMN_SIZE(ctr.*)) AS filesize
FROM
    "CalculationTableRegistry" AS ctr
WHERE
    ctr."CalculationId" IN (SELECT calculationid FROM result_views)
