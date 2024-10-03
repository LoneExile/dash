WITH Result AS (
    SELECT "ResultId"
    FROM
        "ResultIndex"
    WHERE
        "CalculationId" IN ({{ ID_LIST }})
)

DELETE FROM "{{ CURRENT_DIR }}"
WHERE
    "ResultId" IN (SELECT "ResultId" FROM Result)
