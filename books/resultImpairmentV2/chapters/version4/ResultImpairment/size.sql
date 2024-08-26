WITH cal_metrics AS (
    SELECT m."CalculationId" AS calculationid
    FROM
        "ResultImpairment" AS m
    WHERE
        "CalculationId" IN ({{ ID_LIST }})
)

SELECT sum(pg_column_size(m.*)) AS filesize_metrics
FROM
    "ResultImpairment" AS m
WHERE
    m."CalculationId" IN (
        SELECT calculationid
        FROM
            cal_metrics
    )
