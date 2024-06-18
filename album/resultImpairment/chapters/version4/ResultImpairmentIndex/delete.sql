DELETE FROM "ResultImpairmentIndex"
WHERE "CalculationId" IN (
        SELECT
            "CalculationId" AS calculationid
        FROM
            "Calculations"
        WHERE
            "Data"::json ->> 'calculationSpecVersion' = '4');

-- DELETE FROM "ResultImpairmentIndex"
-- WHERE ctid = ANY (ARRAY (
--             SELECT
--                 ctid
--             FROM
--                 "ResultImpairmentIndex"
--             WHERE
--                 "CalculationId" = '{{ ID }}'
--             LIMIT {{ chunkSize }}));
