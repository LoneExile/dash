WITH calculation_table_registry AS (
    SELECT "PointId"
    FROM
        "ResultTransitionMatrix"
    WHERE
        "CalculationId" IN ({{ ID_LIST }})
)

SELECT
    SUM(PG_COLUMN_SIZE(ctr.*)) AS filesize
FROM
    "{{ CURRENT_DIR }}" AS ctr
WHERE
    ctr."PointId" IN (SELECT "PointId" FROM calculation_table_registry)
