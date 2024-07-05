WITH cal_metrics AS (
    SELECT m."CalculationId" AS calculationid
    FROM
        "CalculationMetrics" AS m
    WHERE
        m."CalculationId" IN ({{ ID_LIST }})
)

SELECT SUM(PG_COLUMN_SIZE(m.*)) AS filesize_metrics
FROM
    "CalculationMetrics" AS m
WHERE
    m."CalculationId" IN (
        SELECT calculationid
        FROM
            cal_metrics
    )
