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
    "{{ CURRENT_DIR }}"
WHERE
    "ConsolidatedTransitionId" IN (SELECT "ConsolidatedTransitionId" FROM output)
