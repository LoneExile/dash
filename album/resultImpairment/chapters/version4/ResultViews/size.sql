WITH cal AS (
    SELECT
        c."CalculationId" AS calculationId
    FROM
        "Calculations" c
    WHERE
        c."Data"::json ->> 'calculationSpecVersion' = '4'
),

 result_views AS (
     SELECT
         "CalculationId" AS calculationId
     FROM
         "ResultViews"
     WHERE
         "CalculationId" IN (SELECT calculationId FROM cal)
 )

 SELECT
     -- 'ResultViews' AS table_name,
     -- SUM(pg_column_size(rv)) AS filesize --,
     SUM(pg_column_size(rv)) AS filesize --,
     -- COUNT(*) AS row_count
 FROM
     "ResultViews" AS rv
 WHERE
     rv."CalculationId" IN (SELECT calculationId FROM result_views)
