WITH output AS (
    SELECT "ResultId"
    FROM
        "ResultIndex"
    WHERE
        "CalculationId" IN ({{ ID_LIST }})
),

seg AS (
    SELECT "CollectionId"
    FROM
        "ResultSegmentCollections"
    WHERE
        "ResultId" IN (SELECT "ResultId" FROM output)
)

DELETE FROM "{{ CURRENT_DIR }}"
WHERE
    "CollectionId" IN (SELECT "CollectionId" FROM seg)
