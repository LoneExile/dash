WITH output AS (
    SELECT "ConsolidatedTransitionId"
    FROM
        "ResultConsolidatedTransition"
    WHERE
        "CalculationId" IN ({{ ID_LIST }})
)

SELECT
    SUM(PG_COLUMN_SIZE(ctr.*)) AS filesize
FROM
    "{{ CURRENT_DIR }}" AS ctr
WHERE
    ctr."ConsolidatedTransitionId" IN (SELECT "ConsolidatedTransitionId" FROM output)
