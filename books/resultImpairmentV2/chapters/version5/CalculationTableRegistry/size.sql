WITH calculation_table_registry AS (
    SELECT "CalculationId" AS calculationid
    FROM
        "CalculationTableRegistry"
    WHERE
        "CalculationId" IN ({{ ID_LIST }})
)

SELECT SUM(PG_COLUMN_SIZE(ctr.*)) AS filesize
FROM
    "CalculationTableRegistry" AS ctr
WHERE
    ctr."CalculationId" IN (SELECT calculationid FROM calculation_table_registry)
