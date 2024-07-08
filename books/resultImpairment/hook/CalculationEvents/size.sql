SELECT
    SUM(PG_COLUMN_SIZE(cl.*)) AS filesize
FROM
    "CalculationEvents" AS cl
WHERE
    cl."CalculationId" IN ({{ ID_LIST }})
