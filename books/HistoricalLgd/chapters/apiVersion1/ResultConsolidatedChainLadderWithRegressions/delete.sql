WITH Result AS (
    SELECT "ConsolidatedChainLadderWithRegressionId"
    FROM
         "ResultConsolidatedChainLadderWithRegressionDefinitions"
    WHERE
        "CalculationId" IN ({{ ID_LIST }})
)

DELETE FROM "{{ CURRENT_DIR }}"
WHERE
    "ConsolidatedChainLadderWithRegressionId" IN (SELECT "ConsolidatedChainLadderWithRegressionId" FROM Result)
