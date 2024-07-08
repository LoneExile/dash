WITH cal_metrics AS (
    SELECT m."CalculationId" AS calculationid
    FROM
        "ResultSegmentImpairment" AS m
    WHERE
        "CalculationId" IN ({{ ID_LIST }})
)

SELECT SUM(PG_COLUMN_SIZE(m.*)) AS filesize_metrics
FROM
    "ResultSegmentImpairment" AS m
WHERE
    m."CalculationId" IN (
        SELECT calculationid
        FROM
            cal_metrics
    )
