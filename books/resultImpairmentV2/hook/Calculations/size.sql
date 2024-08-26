SELECT
    SUM(PG_COLUMN_SIZE(cl.*)) AS filesize
FROM
    "Calculations" AS cl
WHERE
    cl."CalculationId" IN ({{ ID_LIST }})
