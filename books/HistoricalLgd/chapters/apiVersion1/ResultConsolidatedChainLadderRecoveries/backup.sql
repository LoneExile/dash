WITH Result AS (
    SELECT "ConsolidatedChainLadderId"
    FROM
        "ResultConsolidatedChainLadderDefinitions"
    WHERE
        "CalculationId" IN ({{ ID_LIST }})
)

SELECT
    *
INTO "{{ BACKUP_TABLE_NAME }}"
FROM
    "{{ CURRENT_DIR }}" AS ctr
WHERE
    ctr."ConsolidatedChainLadderId" IN (SELECT "ConsolidatedChainLadderId" FROM Result)
