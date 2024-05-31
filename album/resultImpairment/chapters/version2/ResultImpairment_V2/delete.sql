DELETE FROM "ResultImpairment_V2"
WHERE ctid = ANY (ARRAY (
            SELECT
                ctid
            FROM
                "ResultImpairment_V2 "
            WHERE
                "CalculationId" = {{ calculationId }}
            LIMIT {{ chunkSize }}));
