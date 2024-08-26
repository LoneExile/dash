SELECT
    SUM(PG_COLUMN_SIZE(cl.*)) AS filesize
FROM
    "CalculationLogs" AS cl
WHERE
    cl."CalculationId" IN ({{ ID_LIST }})
