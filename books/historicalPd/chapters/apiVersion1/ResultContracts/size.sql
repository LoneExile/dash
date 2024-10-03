WITH output AS (
    SELECT "ResultId"
    FROM
        "ResultIndex"
    WHERE
        "CalculationId" IN ({{ ID_LIST }})
)

SELECT
    SUM(PG_COLUMN_SIZE(ctr.*)) AS filesize
FROM
    "{{ CURRENT_DIR }}" AS ctr
WHERE
    ctr."ResultId" IN (SELECT "ResultId" FROM output)
