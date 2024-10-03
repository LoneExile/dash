WITH output AS (
    SELECT "PointId"
    FROM
        "ResultTransitionMatrix"
    WHERE
        "CalculationId" IN ({{ ID_LIST }})
)

SELECT
    *
INTO "{{ BACKUP_TABLE_NAME }}"
FROM
    "{{ CURRENT_DIR }}" AS ctr
WHERE
    ctr."PointId" IN (SELECT "PointId" FROM output)
