WITH Result AS (
    SELECT "ConsolidatedVintageId"
    FROM
         "ResultConsolidatedVintageDefinitions"
    WHERE
        "CalculationId" IN ({{ ID_LIST }})
)

SELECT
    SUM(PG_COLUMN_SIZE(ctr.*)) AS filesize
FROM
    "{{ CURRENT_DIR }}" AS ctr
WHERE
    ctr."ConsolidatedVintageId" IN (SELECT "ConsolidatedVintageId" FROM Result)
