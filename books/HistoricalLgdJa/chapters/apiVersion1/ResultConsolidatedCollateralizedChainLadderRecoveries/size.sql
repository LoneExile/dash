WITH Result AS (
    SELECT "ConsolidatedCollateralizedChainLadderId"
    FROM
         "ResultConsolidatedCollateralizedChainLadderDefinitions"
    WHERE
        "CalculationId" IN ({{ ID_LIST }})
)

SELECT
    SUM(PG_COLUMN_SIZE(ctr.*)) AS filesize
FROM
    "{{ CURRENT_DIR }}" AS ctr
WHERE
    ctr."ConsolidatedCollateralizedChainLadderId" IN (SELECT "ConsolidatedCollateralizedChainLadderId" FROM Result)
