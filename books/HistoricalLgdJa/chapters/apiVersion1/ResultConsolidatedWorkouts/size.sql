WITH Result AS (
    SELECT "ConsolidatedWorkoutId"
    FROM
         "ResultConsolidatedWorkoutDefinitions"
    WHERE
        "CalculationId" IN ({{ ID_LIST }})
)

SELECT
    SUM(PG_COLUMN_SIZE(ctr.*)) AS filesize
FROM
    "{{ CURRENT_DIR }}" AS ctr
WHERE
    ctr."ConsolidatedWorkoutId" IN (SELECT "ConsolidatedWorkoutId" FROM Result)
