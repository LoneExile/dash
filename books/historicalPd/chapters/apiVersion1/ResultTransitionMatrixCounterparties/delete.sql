WITH output AS (
    SELECT "PointId"
    FROM
        "ResultTransitionMatrix"
    WHERE
        "CalculationId" IN ({{ ID_LIST }})
)

DELETE FROM "{{ CURRENT_DIR }}"
WHERE
    "PointId" IN (SELECT "PointId" FROM output)
