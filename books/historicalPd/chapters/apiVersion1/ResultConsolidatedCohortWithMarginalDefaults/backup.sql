WITH output AS (
    SELECT "ConsolidatedTransitionId"
    FROM
        "ResultConsolidatedTransition"
    WHERE
        "CalculationId" IN ({{ ID_LIST }})
)

SELECT
    *
INTO "{{ BACKUP_TABLE_NAME }}"
FROM
    "{{ CURRENT_DIR }}" AS ctr
WHERE
    ctr."ConsolidatedTransitionId" IN (SELECT "ConsolidatedTransitionId" FROM output)
