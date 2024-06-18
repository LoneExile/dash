DELETE FROM "ResultImpairment"
WHERE "CalculationId" IN (
        SELECT
            "CalculationId" AS calculationid
        FROM
            "Calculations"
        WHERE
            "Data"::json ->> 'calculationSpecVersion' = '4');

-- DELETE FROM "ResultImpairment"
-- WHERE ctid = ANY (ARRAY (
--             SELECT
--                 ctid
--             FROM
--                 "ResultImpairment"
--             WHERE
--                 "CalculationId" = '{{ ID }}'
--             LIMIT {{ chunkSize }}));
