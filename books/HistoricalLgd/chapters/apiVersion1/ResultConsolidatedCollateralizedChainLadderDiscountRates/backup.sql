WITH Result AS (
    SELECT "ConsolidatedCollateralizedChainLadderId"
    FROM
         "ResultConsolidatedCollateralizedChainLadderDefinitions"
    WHERE
        "CalculationId" IN ({{ ID_LIST }})
)

SELECT
    *
INTO "{{ BACKUP_TABLE_NAME }}"
FROM
    "{{ CURRENT_DIR }}" AS ctr
WHERE
    ctr."ConsolidatedCollateralizedChainLadderId" IN (SELECT "ConsolidatedCollateralizedChainLadderId" FROM Result)
