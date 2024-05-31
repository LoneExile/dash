DELETE FROM "ResultImpairment"
WHERE ctid = ANY (ARRAY (
            SELECT
                ctid
            FROM
                "ResultImpairment"
            WHERE
                "CalculationId" = '{{ ID }}'
            LIMIT {{ chunkSize }}));
