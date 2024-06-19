WITH cal AS (
    SELECT c."CalculationId" AS calculationid
    FROM
        "Calculations" AS c
    WHERE
        c."Data"::json ->> 'calculationSpecVersion' = '5'
),

calculation_table_registry AS (
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
    ctr."CalculationId" IN (SELECT calculationid FROM calculation_table_registry)
