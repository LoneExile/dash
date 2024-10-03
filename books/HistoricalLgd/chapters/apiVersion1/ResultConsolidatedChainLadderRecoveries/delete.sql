WITH Result AS (
    SELECT "ConsolidatedChainLadderId"
    FROM
        "ResultConsolidatedChainLadderDefinitions"
    WHERE
        "CalculationId" IN ({{ ID_LIST }})
)

DELETE FROM "{{ CURRENT_DIR }}"
WHERE
    "ConsolidatedChainLadderId" IN (SELECT "ConsolidatedChainLadderId" FROM Result)
