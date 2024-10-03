WITH Result AS (
    SELECT "ConsolidatedChainLadderWithRegressionId"
    FROM
         "ResultConsolidatedChainLadderWithRegressionDefinitions"
    WHERE
        "CalculationId" IN ({{ ID_LIST }})
)

SELECT
    *
INTO "{{ BACKUP_TABLE_NAME }}"
FROM
    "{{ CURRENT_DIR }}" AS ctr
WHERE
    ctr."ConsolidatedChainLadderWithRegressionId" IN (SELECT "ConsolidatedChainLadderWithRegressionId" FROM Result)
