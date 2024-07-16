WITH Result AS (
    SELECT "ConsolidatedVintageId"
    FROM
         "ResultConsolidatedVintageDefinitions"
    WHERE
        "CalculationId" IN ({{ ID_LIST }})
)

SELECT
    *
INTO "{{ BACKUP_TABLE_NAME }}"
FROM
    "{{ CURRENT_DIR }}" AS ctr
WHERE
    ctr."ConsolidatedVintageId" IN (SELECT "ConsolidatedVintageId" FROM Result)
