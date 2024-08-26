SELECT
    SUM(PG_COLUMN_SIZE(cl.*)) AS filesize
FROM
    "CombinedCalculations" AS cl
WHERE
    cl."CalculationId" IN ({{ ID_LIST }})
