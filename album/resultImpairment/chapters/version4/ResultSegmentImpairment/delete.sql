DELETE FROM "ResultSegmentImpairment"
WHERE ctid = ANY (ARRAY (
            SELECT
                ctid
            FROM
                "ResultSegmentImpairment"
            WHERE
                "CalculationId" = {{ calculationId }}
            LIMIT {{ chunkSize }}));
