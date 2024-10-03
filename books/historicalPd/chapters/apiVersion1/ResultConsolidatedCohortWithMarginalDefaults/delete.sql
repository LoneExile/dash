WITH output AS (
    SELECT "ConsolidatedTransitionId"
    FROM
        "ResultConsolidatedTransition"
    WHERE
        "CalculationId" IN ({{ ID_LIST }})
)

DELETE FROM "{{ CURRENT_DIR }}"
WHERE "ConsolidatedTransitionId" IN (SELECT "ConsolidatedTransitionId" FROM output)
