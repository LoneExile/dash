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

SELECT
    *
INTO "{{ BACKUP_TABLE_NAME }}"
FROM
    "{{ CURRENT_DIR }}" AS ctr
WHERE
    ctr."CollectionId" IN (SELECT "CollectionId" FROM seg)
