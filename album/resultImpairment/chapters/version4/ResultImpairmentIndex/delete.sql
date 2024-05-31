DELETE FROM "ResultImpairmentIndex"
WHERE ctid = ANY (ARRAY (
            SELECT
                ctid
            FROM
                "ResultImpairmentIndex"
            WHERE
                "CalculationId" = {{ calculationId }}
            LIMIT {{ chunkSize }}));
