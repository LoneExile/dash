WITH Result AS (
    SELECT "ConsolidatedCollateralizedChainLadderId"
    FROM
         "ResultConsolidatedCollateralizedChainLadderDefinitions"
    WHERE
        "CalculationId" IN ({{ ID_LIST }})
)

DELETE FROM "{{ CURRENT_DIR }}"
WHERE
    "ConsolidatedCollateralizedChainLadderId" IN (SELECT "ConsolidatedCollateralizedChainLadderId" FROM Result)
