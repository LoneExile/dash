WITH calculation_table_registry AS (
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
        "ResultId" IN (SELECT "ResultId" FROM calculation_table_registry)
)

SELECT
    SUM(PG_COLUMN_SIZE(ctr.*)) AS filesize
FROM
    "{{ CURRENT_DIR }}" AS ctr
WHERE
    ctr."CollectionId" IN (SELECT "CollectionId" FROM seg)
