WITH Result AS (
    SELECT "ConsolidatedWorkoutId"
    FROM
         "ResultConsolidatedWorkoutDefinitions"
    WHERE
        "CalculationId" IN ({{ ID_LIST }})
)

SELECT
    *
INTO "{{ BACKUP_TABLE_NAME }}"
FROM
    "{{ CURRENT_DIR }}" AS ctr
WHERE
    ctr."ConsolidatedWorkoutId" IN (SELECT "ConsolidatedWorkoutId" FROM Result)
