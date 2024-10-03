WITH Result AS (
    SELECT "ConsolidatedChainLadderWithRegressionId"
    FROM
         "ResultConsolidatedChainLadderWithRegressionDefinitions"
    WHERE
        "CalculationId" IN ({{ ID_LIST }})
)

SELECT
    SUM(PG_COLUMN_SIZE(ctr.*)) AS filesize
FROM
    "{{ CURRENT_DIR }}" AS ctr
WHERE
    ctr."ConsolidatedChainLadderWithRegressionId" IN (SELECT "ConsolidatedChainLadderWithRegressionId" FROM Result)
