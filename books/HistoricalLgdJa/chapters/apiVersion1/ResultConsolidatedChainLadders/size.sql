WITH Result AS (
    SELECT "ConsolidatedChainLadderId"
    FROM
         "ResultConsolidatedChainLadderDefinitions"
    WHERE
        "CalculationId" IN ({{ ID_LIST }})
)

SELECT
    SUM(PG_COLUMN_SIZE(ctr.*)) AS filesize
FROM
    "{{ CURRENT_DIR }}" AS ctr
WHERE
    ctr."ConsolidatedChainLadderId" IN (SELECT "ConsolidatedChainLadderId" FROM Result)
