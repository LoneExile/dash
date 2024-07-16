WITH Result AS (
    SELECT "ConsolidatedVintageId"
    FROM
         "ResultConsolidatedVintageDefinitions"
    WHERE
        "CalculationId" IN ({{ ID_LIST }})
)

DELETE FROM "{{ CURRENT_DIR }}"
WHERE
    "ConsolidatedVintageId" IN (SELECT "ConsolidatedVintageId" FROM Result)
