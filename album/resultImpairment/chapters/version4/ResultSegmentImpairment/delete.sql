DELETE FROM "ResultSegmentImpairment"
WHERE "CalculationId" IN (
        SELECT
            "CalculationId" AS calculationid
        FROM
            "Calculations"
        WHERE
            "Data"::json ->> 'calculationSpecVersion' = '4');

-- DELETE FROM "ResultSegmentImpairment"
-- WHERE ctid = ANY (ARRAY (
--             SELECT
--                 ctid
--             FROM
--                 "ResultSegmentImpairment"
--             WHERE
--                 "CalculationId" = '{{ ID }}'
--             LIMIT {{ chunkSize }}));
