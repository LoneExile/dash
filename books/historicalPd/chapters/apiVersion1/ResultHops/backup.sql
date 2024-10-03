WITH output AS (
    SELECT "ResultId"
    FROM
        "ResultIndex"
    WHERE
        "CalculationId" IN ({{ ID_LIST }})
)

SELECT
    *
INTO "{{ BACKUP_TABLE_NAME }}"
FROM
    "{{ CURRENT_DIR }}" AS ctr
WHERE
    ctr."ResultId" IN (SELECT "ResultId" FROM output)
