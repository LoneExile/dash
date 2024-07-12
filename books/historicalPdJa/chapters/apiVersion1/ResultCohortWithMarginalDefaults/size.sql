-- WITH calculation_table_registry AS (
--     SELECT "PointId"
--     FROM
--         "ResultCohorts"
--     WHERE
--         "CalculationId" IN ({{ ID_LIST }})
-- )

-- SELECT
--     SUM(PG_COLUMN_SIZE(ctr.*)) AS filesize
-- FROM
--     "{{ CURRENT_DIR }}" AS ctr
-- WHERE
--     ctr."PointId" IN (SELECT PointId FROM calculation_table_registry)

SELECT
    SUM(PG_COLUMN_SIZE(ctr.*)) AS filesize
FROM
    "{{ CURRENT_DIR }}" AS ctr
WHERE
    ctr."CalculationId" IN ({{ ID_LIST }})
