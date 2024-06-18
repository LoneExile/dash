WITH cal AS (
    SELECT c."CalculationId" AS calculationid
    FROM
        "Calculations" AS c
    WHERE
        c."Data"::json ->> 'calculationSpecVersion' = '4'
),

cal_metrics AS (
    SELECT m."CalculationId" AS calculationid
    FROM
        "ResultImpairment" AS m
    WHERE
        m."CalculationId" IN (
            SELECT calculationid
            FROM
                cal
        )
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
