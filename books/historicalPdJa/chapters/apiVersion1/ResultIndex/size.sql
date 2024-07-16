WITH output AS (
    SELECT "CalculationId" AS calculationid
    FROM
        "{{ CURRENT_DIR }}"
    WHERE
        "CalculationId" IN ({{ ID_LIST }})
)

SELECT SUM(PG_COLUMN_SIZE(ctr.*)) AS filesize
FROM
    "{{ CURRENT_DIR }}" AS ctr
WHERE
    ctr."CalculationId" IN (SELECT calculationid FROM output)
