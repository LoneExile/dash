WITH cal_metrics AS (
    SELECT m."CalculationId" AS calculationid
    FROM
        "ResultImpairmentIndex" AS m
    WHERE
        "CalculationId" IN ({{ ID_LIST }})
)

SELECT SUM(PG_COLUMN_SIZE(m.*)) AS filesize_metrics
FROM
    "ResultImpairmentIndex" AS m
WHERE
    m."CalculationId" IN (
        SELECT calculationid
        FROM
            cal_metrics
    )
