WITH Result AS (
    SELECT "ConsolidatedWorkoutId"
    FROM
         "ResultConsolidatedWorkoutDefinitions"
    WHERE
        "CalculationId" IN ({{ ID_LIST }})
)

DELETE FROM "{{ CURRENT_DIR }}"
WHERE
    "ConsolidatedWorkoutId" IN (SELECT "ConsolidatedWorkoutId" FROM Result)
