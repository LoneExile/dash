SELECT
    SUM(PG_COLUMN_SIZE(cl.*)) AS filesize
FROM
    "ResultProcessed" AS cl
WHERE
    cl."CalculationId" IN ({{ ID_LIST }})
